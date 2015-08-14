/*
 * Licensed to CRATE.IO GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.executor.transport.distributed;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Streamer;
import io.crate.analyze.WhereClause;
import io.crate.breaker.RamAccountingContext;
import io.crate.executor.transport.Transports;
import io.crate.integrationtests.SQLTransportIntegrationTest;
import io.crate.jobs.JobContextService;
import io.crate.jobs.JobExecutionContext;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.collect.*;
import io.crate.operation.projectors.ResultProvider;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.types.DataTypes;
import io.crate.types.IntegerType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 1)
public class DistributingDownstreamTest extends SQLTransportIntegrationTest {

    private static final String INDEX_NAME = "downstream_test";
    private static final RamAccountingContext RAM_ACCOUNTING_CONTEXT =
            new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.Name.FIELDDATA));

    private final Integer NUMBER_OF_DOCUMENTS = 10_000;

    private JobContextService jobContextService;
    private JobCollectContext jobCollectContext;
    private ExecutorService executorService;


    @Before
    public void prepare() throws Exception {
        execute("create table \"" + INDEX_NAME + "\" (" +
                " id integer " +
                ") with (number_of_replicas=0)");
        refresh();
        generateData();
        jobContextService = internalCluster().getInstance(JobContextService.class);
        executorService = Executors.newScheduledThreadPool(30);
    }

    @After
    public void closeContext() throws Exception {
        if (jobCollectContext != null) {
            jobCollectContext.close();
        }
        executorService.shutdown();
        executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
    }

    private void generateData() {
        Object[][] bulkArgs = new Object[NUMBER_OF_DOCUMENTS][1];
        for (int i = 1; i <= NUMBER_OF_DOCUMENTS; i++) {
            bulkArgs[i-1][0] = i;
        }
        execute("insert into " + INDEX_NAME + " (id) values (?)", bulkArgs);
        //assertThat(response.rowCount(), is(NUMBER_OF_DOCUMENTS.longValue()));
    }

    private void createJob(UUID jobId, ResultProvider projector) throws Exception {
        ReferenceIdent ident = new ReferenceIdent(new TableIdent("doc", INDEX_NAME), "countryName");
        Reference ref = new Reference(new ReferenceInfo(ident, RowGranularity.DOC, DataTypes.STRING));

        DocSchemaInfo docSchemaInfo = internalCluster().getInstance(DocSchemaInfo.class);
        TableInfo tableInfo = docSchemaInfo.getTableInfo(INDEX_NAME);

        CollectPhase node = new CollectPhase(jobId, 0, "collect", tableInfo.getRouting(WhereClause.MATCH_ALL, null), ImmutableList.<Symbol>of(ref), ImmutableList.<Projection>of());
        node.whereClause(WhereClause.MATCH_ALL);
        node.maxRowGranularity(RowGranularity.DOC);

        CollectOperation collectOperation = internalCluster().getInstance(MapSideDataCollectOperation.class);
        JobExecutionContext.Builder builder = jobContextService.newBuilder(jobId);
        jobCollectContext = new JobCollectContext(
                jobId, node, collectOperation, RAM_ACCOUNTING_CONTEXT, projector);
        builder.addSubContext(node.executionPhaseId(), jobCollectContext);
        jobContextService.createContext(builder);
    }

    public void testPauseResume() throws Exception {
        UUID jobId = UUID.randomUUID();

        final AtomicBoolean shouldBlockResponse = new AtomicBoolean(true);
        final CountDownLatch rows = new CountDownLatch(NUMBER_OF_DOCUMENTS);
        final Semaphore blockResponse = new Semaphore(1);
        blockResponse.acquire();
        try {
            TransportDistributedResultAction distributedResultAction = spy(new TransportDistributedResultAction(
                    mock(Transports.class),
                    mock(JobContextService.class),
                    mock(ThreadPool.class),
                    mock(TransportService.class)) {

                @Override
                public void pushResult(String node, final DistributedResultRequest request, final ActionListener<DistributedResultResponse> listener) {
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            if (shouldBlockResponse.get()) {
                                try {
                                    blockResponse.acquire();
                                } catch (InterruptedException e) {
                                    listener.onFailure(e);
                                    return;
                                }
                            }
                            for (int i = 0; i < request.rows().size(); i++) {
                                rows.countDown();
                            }
                            listener.onResponse(new DistributedResultResponse(!request.isLast()));
                        }
                    });
                }
            });


            final SettableFuture<Boolean> waitForPause = SettableFuture.create();
            final SettableFuture<Boolean> waitForResume = SettableFuture.create();
            DistributingDownstream distributingDownstream = new BroadcastDistributingDownstream(
                    jobId,
                    0, (byte) 0, 0,
                    ImmutableList.of("dummy"),
                    distributedResultAction,
                    new Streamer[]{IntegerType.INSTANCE.streamer()},
                    ImmutableSettings.EMPTY,
                    2
            ) {
                @Override
                public void pause() {
                    super.pause();
                    waitForPause.set(true);
                }

                @Override
                public void resume(boolean async) {
                    super.resume(async);
                    waitForResume.set(true);
                }
            };

            createJob(jobId, distributingDownstream);
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    jobCollectContext.start();
                }
            });

            waitForPause.get(10, TimeUnit.SECONDS);

            final List<LuceneDocCollector> collectors = new ArrayList<>();
            java.lang.reflect.Field jobCollectors = JobCollectContext.class.getDeclaredField("collectors");
            jobCollectors.setAccessible(true);
            for (CrateCollector collector : (Collection<CrateCollector>) jobCollectors.get(jobCollectContext)) {
                collectors.add((LuceneDocCollector) collector);
            }


            final java.lang.reflect.Field paused = LuceneDocCollector.class.getDeclaredField("paused");
            paused.setAccessible(true);
            for (final LuceneDocCollector collector : collectors) {
                assertBusy(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            assertThat(((AtomicBoolean) paused.get(collector)).get(), is(true));
                        } catch (IllegalAccessException e) {
                            fail("IllegalAccessException: " + e.getMessage());
                        }

                    }
                });
            }

            shouldBlockResponse.set(false);
            blockResponse.release();

            waitForResume.get(10, TimeUnit.SECONDS);

            rows.await(10, TimeUnit.SECONDS);

        } finally {
            blockResponse.release();
        }
    }
}
