/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.operation.projectors;

import com.google.common.collect.Sets;
import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowUpstream;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class PassThroughRowMergers {

    private PassThroughRowMergers() {}

    public static RowDownstream rowMerger(RowReceiver delegate) {
        MultiUpstreamRowReceiver rowReceiver;
        if (delegate.requiresRepeatSupport()) {
            rowReceiver = new RowCachingMultiUpstreamRowReceiver(delegate);
        } else {
            rowReceiver = new MultiUpstreamRowReceiver(delegate);
        }
        return new MultiUpstreamRowMerger(rowReceiver);
    }

    private static class MultiUpstreamRowMerger implements RowMerger {

        private final MultiUpstreamRowReceiver rowReceiver;
        private boolean repeated = false;

        public MultiUpstreamRowMerger(MultiUpstreamRowReceiver rowReceiver) {
            this.rowReceiver = rowReceiver;
            rowReceiver.delegate.setUpstream(this);
        }

        @Override
        public RowReceiver newRowReceiver() {
            rowReceiver.activeUpstreams.incrementAndGet();
            return rowReceiver;
        }

        @Override
        public void pause() {
            for (RowUpstream rowUpstream : rowReceiver.rowUpstreams) {
                rowUpstream.pause();
            }
        }

        @Override
        public void resume(boolean async) {
            for (RowUpstream rowUpstream : rowReceiver.rowUpstreams) {
                rowUpstream.resume(async);
            }
        }

        @Override
        public void repeat() {
            Preconditions.checkState(!repeated,
                    "Row receiver should have changed it's upstream after the first repeat call");
            repeated = true;
            // the rowEmitter becomes the new upstream for rowReceiver.delegate and handles further pause/resume/repeat calls
            IterableRowEmitter iterableRowEmitter = new IterableRowEmitter(
                    rowReceiver.delegate,
                    rowReceiver.executionState,
                    rowReceiver.cachedRows());
            iterableRowEmitter.run();
        }
    }

    static class RowCachingMultiUpstreamRowReceiver extends MultiUpstreamRowReceiver {

        private List<Object[]> rows = new ArrayList<>();

        public RowCachingMultiUpstreamRowReceiver(RowReceiver delegate) {
            super(delegate);
        }

        @Override
        public List<Object[]> cachedRows() {
            return rows;
        }

        @Override
        public boolean setNextRow(Row row) {
            synchronized (delegate) {
                rows.add(row.materialize());
                return delegate.setNextRow(row);
            }
        }
    }

    static class MultiUpstreamRowReceiver implements RowReceiver {

        private static final ESLogger LOGGER = Loggers.getLogger(MultiUpstreamRowReceiver.class);

        ExecutionState executionState;
        final RowReceiver delegate;
        final Set<RowUpstream> rowUpstreams = Sets.newConcurrentHashSet();
        private final AtomicInteger activeUpstreams = new AtomicInteger(0);
        private final AtomicBoolean prepared = new AtomicBoolean(false);
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final Object lock = new Object();

        public MultiUpstreamRowReceiver(RowReceiver delegate) {
            this.delegate = delegate;
        }

        public List<Object[]> cachedRows() {
            throw new UnsupportedOperationException("Doesn't cache rows");
        }

        @Override
        public boolean setNextRow(Row row) {
            synchronized (lock) {
                return delegate.setNextRow(row);
            }
        }

        @Override
        public void finish() {
            countdown();
        }

        @Override
        public void fail(Throwable throwable) {
            failure.set(throwable);
            countdown();
        }

        @Override
        public void prepare(ExecutionState executionState) {
            if (prepared.compareAndSet(false, true)) {
                this.executionState = executionState;
                delegate.prepare(executionState);
            }
        }

        @Override
        public boolean requiresRepeatSupport() {
            return false;
        }

        @Override
        public void setUpstream(RowUpstream rowUpstream) {
            if (!rowUpstreams.add(rowUpstream)) {
                LOGGER.debug("Upstream {} registered itself twice", rowUpstream);
            }
        }

        private void countdown() {
            if (activeUpstreams.decrementAndGet() == 0) {
                Throwable t = failure.get();
                if (t == null) {
                    delegate.finish();
                } else {
                    delegate.fail(t);
                }
            }
        }
    }
}
