/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.collect;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.exceptions.UnhandledServerException;
import io.crate.metadata.Functions;
import io.crate.metadata.NestedReferenceResolver;
import io.crate.operation.ThreadPools;
import io.crate.operation.collect.sources.CollectSource;
import io.crate.operation.collect.sources.CollectSourceResolver;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.operation.reference.sys.node.NodeSysReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * collect local data from node/shards/docs on nodes where the data resides (aka Mapper nodes)
 */
@Singleton
public class MapSideDataCollectOperation {

    private final EvaluatingNormalizer clusterNormalizer;
    private final ClusterService clusterService;
    private final CollectSourceResolver collectSourceResolver;
    private final ThreadPoolExecutor executor;
    private final int poolSize;
    private final Functions functions;
    private final NodeSysExpression nodeSysExpression;

    @Inject
    public MapSideDataCollectOperation(ClusterService clusterService,
                                       Functions functions,
                                       NestedReferenceResolver clusterReferenceResolver,
                                       NodeSysExpression nodeSysExpression,
                                       ThreadPool threadPool,
                                       CollectSourceResolver collectSourceResolver) {
        this.functions = functions;
        this.nodeSysExpression = nodeSysExpression;
        this.executor = (ThreadPoolExecutor) threadPool.executor(ThreadPool.Names.SEARCH);
        this.poolSize = executor.getMaximumPoolSize();
        this.clusterService = clusterService;
        this.collectSourceResolver = collectSourceResolver;

        clusterNormalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, clusterReferenceResolver);
    }

    /**
     * dispatch by the following criteria:
     * <p>
     * * if local node id is contained in routing:<br>
     * * if no shards are given:<br>
     * &nbsp; -&gt; run row granularity level collect<br>
     * &nbsp; except for doc level:
     * &nbsp; &nbsp; if table if partitioned:
     * &nbsp; &nbsp; -&gt; edge case for empty partitioned table
     * &nbsp; &nbsp; else:
     * &nbsp; &nbsp; -&gt; collect from information schema
     * * if shards are given:<br>
     * &nbsp; -&gt; run shard or doc level collect<br>
     * * else if we got cluster RowGranularity:<br>
     * &nbsp; -&gt; run node level collect (cluster level)<br>
     * </p>
     */
    public Collection<CrateCollector> createCollectors(CollectPhase collectPhase,
                                                       RowReceiver downstream,
                                                       final JobCollectContext jobCollectContext) {
        assert collectPhase.isRouted(); // not routed collect is not handled here
        assert collectPhase.jobId() != null : "no jobId present for collect operation";

        String localNodeId = clusterService.state().nodes().localNodeId();
        Set<String> routingNodes = collectPhase.routing().nodes();

        if (!routingNodes.contains(localNodeId)) {
            throw new UnhandledServerException("unsupported routing");
        } else {
            CollectSource service = collectSourceResolver.getService(collectPhase, localNodeId);
            collectPhase = normalize(collectPhase);
            return service.getCollectors(collectPhase, downstream, jobCollectContext);
        }
    }

    private CollectPhase normalize(CollectPhase collectPhase) {
        collectPhase = collectPhase.normalize(clusterNormalizer);
        switch (collectPhase.maxRowGranularity()) {
            case NODE:
            case DOC:
                EvaluatingNormalizer normalizer =
                        new EvaluatingNormalizer(functions, RowGranularity.NODE, new NodeSysReferenceResolver(nodeSysExpression));
                return collectPhase.normalize(normalizer);
        }
        return collectPhase;
    }

    public void launchCollectors(CollectPhase collectPhase,
                                 final Collection<CrateCollector> shardCollectors) throws RejectedExecutionException {
        assert !shardCollectors.isEmpty();
        if (collectPhase.maxRowGranularity() == RowGranularity.SHARD) {
            // run sequential to prevent sys.shards queries from using too many threads
            // and overflowing the threadpool queues
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    for (CrateCollector collector : shardCollectors) {
                        collector.doCollect();
                    }
                }
            });
        } else {
            ThreadPools.runWithAvailableThreads(
                    executor,
                    poolSize,
                    collectors2Runnables(shardCollectors));
        }
    }

    private Collection<Runnable> collectors2Runnables(Collection<CrateCollector> collectors) {
        return Collections2.transform(collectors, new Function<CrateCollector, Runnable>() {
            @Override
            public Runnable apply(final CrateCollector collector) {
                return new Runnable() {
                    @Override
                    public void run() {
                        collector.doCollect();
                    }
                };
            }
        });
    }
}
