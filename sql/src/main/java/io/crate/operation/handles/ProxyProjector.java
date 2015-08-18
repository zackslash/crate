/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.operation.handles;

import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowDownstream;
import io.crate.operation.RowDownstreamHandle;
import io.crate.operation.RowUpstream;
import io.crate.operation.projectors.Projector;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class ProxyProjector implements Projector, RowDownstreamHandle {

    private final List<RowUpstream> upstreams = new ArrayList<>(1);
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final AtomicInteger activeUpstreams = new AtomicInteger(0);
    private final HandleFactory handleFactory;

    private RowDownstreamHandle delegateDownstream = NO_OP_HANDLE;
    private RowDownstreamHandle delegate = RowDownstreamHandle.NO_OP_HANDLE;

    public ProxyProjector(HandleFactory handleFactory) {
        this.handleFactory = handleFactory;
    }

    @Override
    public void startProjection(ExecutionState executionState) {
        delegate = handleFactory.create(executionState, delegateDownstream);
    }

    @Override
    public void downstream(RowDownstream downstream) {
        delegateDownstream = downstream.registerUpstream(this);
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        activeUpstreams.incrementAndGet();
        upstreams.add(upstream);
        return this;
    }

    @Override
    public void pause() {
        for (RowUpstream upstream : upstreams) {
            upstream.pause();
        }
    }

    @Override
    public void resume(boolean async) {
        for (RowUpstream upstream : upstreams) {
            upstream.resume(async);
        }
    }

    @Override
    public boolean setNextRow(Row row) {
        return delegate.setNextRow(row);
    }

    @Override
    public void finish() {
        int upstreams = activeUpstreams.decrementAndGet();
        if (upstreams == 0) {
            Throwable throwable = failure.get();
            if (throwable == null) {
                delegate.finish();
            } else {
                delegate.fail(throwable);
            }
        }
        assert upstreams >= 0 : "upstreams may not get negative";
    }

    @Override
    public void fail(Throwable throwable) {
        int upstreams = activeUpstreams.decrementAndGet();
        if (upstreams == 0) {
            delegate.fail(throwable);
        } else {
            failure.set(throwable);
        }
        assert upstreams >= 0 : "upstreams may not get negative";
    }
}
