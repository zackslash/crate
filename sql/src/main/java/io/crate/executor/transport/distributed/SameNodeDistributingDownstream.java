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

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.core.collections.Bucket;
import io.crate.core.collections.CollectionBucket;
import io.crate.core.collections.Row;
import io.crate.jobs.*;
import io.crate.operation.PageResultListener;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.ResultProvider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * A result provider implementation which is sending all rows to a local
 * {@link io.crate.jobs.PageDownstreamContext}.
 *
 * This class do not operate thread-safe!
 */
public class SameNodeDistributingDownstream implements ResultProvider {

    private static final ESLogger LOGGER = Loggers.getLogger(SameNodeDistributingDownstream.class);

    private final UUID jobId;
    private final int targetExecutionNodeId;
    private final byte targetExecutionNodeInputId;
    private final JobContextService jobContextService;
    private final PageResultListener pageResultListener;
    private final List<Object[]> rows = new ArrayList<>(Constants.PAGE_SIZE);
    private final Bucket bucket = new CollectionBucket(rows);
    private final SettableFuture<Bucket> result = SettableFuture.create();

    private PageDownstreamContext downstreamContext;
    private volatile boolean consumedRows = true;
    private final AtomicInteger registeredUpstreams = new AtomicInteger(0);

    public SameNodeDistributingDownstream(UUID jobId,
                                          int targetExecutionNodeId,
                                          byte targetExecutionNodeInputId,
                                          JobContextService jobContextService) {
        this.jobId = jobId;
        this.targetExecutionNodeId = targetExecutionNodeId;
        this.targetExecutionNodeInputId = targetExecutionNodeInputId;
        this.jobContextService = jobContextService;
        pageResultListener = new PageResultListener() {
            @Override
            public void needMore(boolean needMore) {
                if (needMore && registeredUpstreams.get() == 0) {
                    LOGGER.trace("needMore: sendBucket");
                    sendBucket();
                }
            }

            @Override
            public int buckedIdx() {
                return 0;
            }
        };
        LOGGER.setLevel("trace");
    }

    private void setUpDownstreamContext() {
        JobExecutionContext jobExecutionContext = jobContextService.getContext(jobId);
        DownstreamExecutionSubContext executionSubContext = jobExecutionContext.getSubContext(targetExecutionNodeId);
        downstreamContext = executionSubContext.pageDownstreamContext(targetExecutionNodeInputId);
        if (downstreamContext == null) {
            throw new IllegalStateException(String.format(Locale.ENGLISH,
                    "Couldn't find pageDownstreamContext for input %d", targetExecutionNodeInputId));
        }
        downstreamContext.addCallback(new ContextCallback() {
            @Override
            public void onClose(@Nullable Throwable error, long bytesUsed) {
                consumedRows = false;
            }
        });
    }

    @Override
    public ListenableFuture<Bucket> result() {
        if (downstreamContext == null) {
            setUpDownstreamContext();
        }
        return result;
    }

    @Override
    public void startProjection(ExecutionState executionState) {
    }

    @Override
    public void downstream(RowDownstream downstream) {
        throw new UnsupportedOperationException("Setting a downstream is not supported here");
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        registeredUpstreams.incrementAndGet();
        return this;
    }

    @Override
    public boolean setNextRow(Row row) {
        if (!consumedRows) {
            return false;
        }
        synchronized (rows) {
            LOGGER.trace("Adding row");
            rows.add(row.materialize());
            sendBucket();
        }

        return true;
    }

    @Override
    public void finish() {
        LOGGER.trace("finished");
        registeredUpstreams.decrementAndGet();
        synchronized (rows) {
            sendBucket();
        }
        downstreamContext.finish();
        result.set(null);
    }

    @Override
    public void fail(Throwable throwable) {
        downstreamContext.failure(0, throwable);
        result.setException(throwable);
    }

    private void sendBucket() {
        if (rows.size() == Constants.PAGE_SIZE || registeredUpstreams.get() == 0) {
            LOGGER.trace("sending bucket, finished? {}", registeredUpstreams.get() == 0);
            downstreamContext.setBucket(0, bucket, registeredUpstreams.get() == 0, pageResultListener);
            rows.clear();
        }
    }
}
