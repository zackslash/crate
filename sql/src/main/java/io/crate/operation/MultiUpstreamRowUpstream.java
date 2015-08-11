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

package io.crate.operation;

public class MultiUpstreamRowUpstream implements RowUpstream {

    private final MultiUpstreamRowDownstream multiUpstreamRowDownstream;

    public MultiUpstreamRowUpstream(MultiUpstreamRowDownstream multiUpstreamRowDownstream) {
        this.multiUpstreamRowDownstream = multiUpstreamRowDownstream;
    }

    @Override
    public void pause() {
        if (multiUpstreamRowDownstream.upstreamsRunning()) {
            for (RowUpstream upstream : multiUpstreamRowDownstream.upstreams()) {
                upstream.pause();
            }
        }
    }

    @Override
    public void resume(boolean async) {
        if (multiUpstreamRowDownstream.upstreamsRunning()) {
            int i = 1;
            for (RowUpstream upstream : multiUpstreamRowDownstream.upstreams()) {
                if (async && i == multiUpstreamRowDownstream.upstreams().size()) {
                    // last upstream is started synchronous
                    upstream.resume(false);
                } else {
                    upstream.resume(async);
                }
                i++;
            }
        }
   }
}