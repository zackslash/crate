/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Lists;
import io.crate.core.collections.Row;
import io.crate.core.collections.RowDelegate;
import io.crate.core.collections.RowN;
import io.crate.executor.transport.*;
import io.crate.jobs.ExecutionState;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.operation.*;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.fetch.FetchOutputBucketMerger;
import io.crate.operation.fetch.MultiRelationRow;
import io.crate.operation.fetch.PositionalRowDelegate;
import io.crate.operation.fetch.RowInputSymbolVisitor;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.shard.ShardId;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FetchProjector implements Projector, RowDownstreamHandle {

    public static final int NO_BULK_REQUESTS = -1;

    private FetchOutputBucketMerger downstream;
    private final TransportFetchNodeAction transportFetchNodeAction;
    private final TransportCloseContextNodeAction transportCloseContextNodeAction;

    private final UUID jobId;
    private final List<FetchRelation> fetchRelations;
    private final IntObjectOpenHashMap<Integer> jobSearchContextIdToExecutionNodeId;
    private final IntObjectOpenHashMap<String> jobSearchContextIdToNode;
    private final IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard;
    private final int bulkSize;
    private final boolean closeContexts;
    private final MultiRelationRow outputRow;
    private final Map<Integer, NodeBucket> nodeBuckets = new HashMap<>();
    private final AtomicInteger remainingUpstreams = new AtomicInteger(0);
    private final AtomicBoolean consumingRows = new AtomicBoolean(true);
    private int numNodes = 0;
    private final AtomicInteger remainingRequests = new AtomicInteger(0);
    private final Map<String, Row> partitionRowsCache = new HashMap<>();
    private final Object partitionRowsCacheLock = new Object();
    private final Map<Integer, Integer> upstreamIdToRelationId = new HashMap<>();
    private final List<String> executionNodes;

    private int inputCursor = 0;
    private boolean consumedRows = false;

    private static final ESLogger LOGGER = Loggers.getLogger(FetchProjector.class);

    public FetchProjector(TransportFetchNodeAction transportFetchNodeAction,
                          TransportCloseContextNodeAction transportCloseContextNodeAction,
                          UUID jobId,
                          List<FetchRelation> fetchRelations,
                          List<Symbol> outputSymbols,
                          Set<String> executionNodes,
                          IntObjectOpenHashMap<Integer> jobSearchContextIdToExecutionNodeId,
                          IntObjectOpenHashMap<String> jobSearchContextIdToNode,
                          IntObjectOpenHashMap<ShardId> jobSearchContextIdToShard,
                          int bulkSize,
                          boolean closeContexts) {
        this.transportFetchNodeAction = transportFetchNodeAction;
        this.transportCloseContextNodeAction = transportCloseContextNodeAction;
        this.jobId = jobId;
        this.fetchRelations = fetchRelations;
        this.jobSearchContextIdToExecutionNodeId = jobSearchContextIdToExecutionNodeId;
        this.jobSearchContextIdToNode = jobSearchContextIdToNode;
        this.jobSearchContextIdToShard = jobSearchContextIdToShard;
        this.bulkSize = bulkSize;
        this.closeContexts = closeContexts;

        this.executionNodes = Lists.newArrayList(executionNodes);
        numNodes = executionNodes.size();

        for (int i = 0; i < numNodes; i++) {
            for (int j = 0; j < fetchRelations.size(); j++) {
                upstreamIdToRelationId.put(i, j);
            }
        }

        List<Input<?>> outputInputs = new ArrayList<>(outputSymbols.size());
        for (Symbol symbol : outputSymbols) {
            for (FetchRelation fetchRelation : fetchRelations) {
                Input<?> input = fetchRelation.processOutputSymbol(symbol);
                if (input != null) {
                    outputInputs.add(input);
                }
            }
        }

        outputRow = new MultiRelationRow(outputInputs, fetchRelations.size());
    }

    @Override
    public void startProjection(ExecutionState executionState) {
        for (FetchRelation fetchRelation : fetchRelations) {
            fetchRelation.collectDocIdExpression().startCollect();
        }

        if (remainingUpstreams.get() <= 0) {
            finish();
        } else {
            // register once to increment downstream upstreams counter
            downstream.registerUpstream(this);
        }
    }

    @Override
    public synchronized boolean setNextRow(Row row) {
        if (!consumingRows.get()) {
            return false;
        }
        consumedRows = true;

        for (int i = 0; i < fetchRelations.size(); i++) {
            FetchRelation fetchRelation = fetchRelations.get(i);
            fetchRelation.collectDocIdExpression().setNextRow(row);

            long docId = (Long) fetchRelation.collectDocIdExpression().value();
            int jobSearchContextId = (int)(docId >> 32);

            String nodeId = jobSearchContextIdToNode.get(jobSearchContextId);
            String index = jobSearchContextIdToShard.get(jobSearchContextId).getIndex();
            Integer executionNodeId = jobSearchContextIdToExecutionNodeId.get(jobSearchContextId);
            int nodeIdIndex = Objects.hash(nodeId, executionNodeId);

            NodeBucket nodeBucket = nodeBuckets.get(nodeIdIndex);
            if (nodeBucket == null) {
                nodeBucket = new NodeBucket(nodeId,
                        executionNodes.indexOf(nodeId), executionNodeId,
                        fetchRelation, i);
                nodeBuckets.put(nodeIdIndex, nodeBucket);

            }
            Row partitionRow = partitionedByRow(index, fetchRelation.partitionedBy());
            nodeBucket.add(inputCursor++, docId, partitionRow, row);
            if (bulkSize != NO_BULK_REQUESTS && nodeBucket.size() >= bulkSize) {
                flushNodeBucket(nodeBucket);
                nodeBuckets.remove(nodeIdIndex);
            }
        }


        return true;
    }

    @Override
    public void downstream(RowDownstream downstream) {
        this.downstream = new FetchOutputBucketMerger(downstream, numNodes * fetchRelations.size(), outputRow, upstreamIdToRelationId);
    }

    @Override
    public RowDownstreamHandle registerUpstream(RowUpstream upstream) {
        remainingUpstreams.incrementAndGet();
        return this;
    }

    @Override
    public void finish() {
        if (remainingUpstreams.decrementAndGet() == 0) {
            // flush all remaining buckets
            Iterator<NodeBucket> it = nodeBuckets.values().iterator();
            while (it.hasNext()) {
                flushNodeBucket(it.next());
                it.remove();
            }

            // projector registered itself as an upstream to prevent downstream of
            // flushing rows before all requests finished.
            // release it now as no new rows are consumed anymore (downstream will flush all remaining rows)
            downstream.finish();

            // no rows consumed (so no fetch requests made), but collect contexts are open, close them.
            if (!consumedRows) {
                closeContexts();
            }
        }
    }

    @Override
    public void fail(Throwable throwable) {
        downstream.fail(throwable);
    }

    @Nullable
    private Row partitionedByRow(String index, List<ReferenceInfo> partitionedBy) {
        synchronized (partitionRowsCacheLock) {
            if (partitionRowsCache.containsKey(index)) {
                return partitionRowsCache.get(index);
            }
        }
        Row partitionValuesRow = null;
        if (!partitionedBy.isEmpty() && PartitionName.isPartition(index)) {
            Object[] partitionValues;
            List<BytesRef> partitionRowValues = PartitionName.fromStringSafe(index).values();
            partitionValues = new Object[partitionRowValues.size()];
            for (int i = 0; i < partitionRowValues.size(); i++) {
                partitionValues[i] = partitionedBy.get(i).type().value(partitionRowValues.get(i));
            }
            partitionValuesRow = new RowN(partitionValues);
        }
        synchronized (partitionRowsCacheLock) {
            partitionRowsCache.put(index, partitionValuesRow);
        }
        return partitionValuesRow;
    }

    private void flushNodeBucket(final NodeBucket nodeBucket) {
        // TODO: support multiple relations in one request/response

        // every request must increase downstream upstream counter
        downstream.registerUpstream(this);
        remainingRequests.incrementAndGet();

        final NodeFetchRequest request = new NodeFetchRequest();
        request.jobId(jobId);
        request.executionNodeId(nodeBucket.executionNodeId);
        request.toFetchReferences(nodeBucket.fetchRelation().fetchReferences());
        request.jobSearchContextDocIds(nodeBucket.docIds());
        if (bulkSize > NO_BULK_REQUESTS) {
            request.closeContext(false);
        }
        transportFetchNodeAction.execute(nodeBucket.nodeId, request, new ActionListener<NodeFetchResponse>() {
            @Override
            public void onResponse(NodeFetchResponse response) {
                List<Row> rows = new ArrayList<>(response.rows().size());
                int idx = 0;
                for (Row row : response.rows()) {
                    nodeBucket.fetchRelation().processFetchRow(row, nodeBucket.inputRow(idx),
                            nodeBucket.partitionRow(idx));
                    try {
                        rows.add(new PositionalRowDelegate(nodeBucket.fetchRelation().outputRow(), nodeBucket.cursor(idx)));
                    } catch (Throwable e) {
                        onFailure(e);
                        return;
                    }
                    idx++;
                }
                int upstreamBucketId = nodeBucket.nodeIdx * fetchRelations.size() + nodeBucket.relationIdx;
                if (!downstream.setNextBucket(rows, upstreamBucketId)) {
                    consumingRows.set(false);
                }
                if (remainingRequests.decrementAndGet() <= 0 && remainingUpstreams.get() <= 0) {
                    closeContexts();
                }
                downstream.finish();
            }

            @Override
            public void onFailure(Throwable e) {
                consumingRows.set(false);
                downstream.fail(e);
            }
        });

    }

    /**
     * close job contexts on all affected nodes, just fire & forget, they will timeout anyway
     */
    private void closeContexts() {
        if (closeContexts || bulkSize > NO_BULK_REQUESTS) {
            LOGGER.trace("closing job context {} on {} nodes", jobId, numNodes);

            int[] executionNodeIds = new int[fetchRelations.size()];
            for (int i = 0; i < fetchRelations.size(); i++) {
                executionNodeIds[i] = fetchRelations.get(i).executionNodeId();
            }

            for (final String nodeId : executionNodes) {
                transportCloseContextNodeAction.execute(nodeId,
                        new NodeCloseContextRequest(jobId, executionNodeIds),
                        new ActionListener<NodeCloseContextResponse>() {
                            @Override
                            public void onResponse(NodeCloseContextResponse nodeCloseContextResponse) {
                            }

                            @Override
                            public void onFailure(Throwable e) {
                                LOGGER.warn("Closing job context {} failed on node {} with: {}", e, jobId, nodeId, e.getMessage());
                            }
                        });
            }
        }
    }

    private static class NodeBucket {

        private final int nodeIdx;
        private final String nodeId;
        private final List<Row> partitionRows = new ArrayList<>();
        private final List<Row> inputRows = new ArrayList<>();
        private final IntArrayList cursors = new IntArrayList();
        private final LongArrayList docIds = new LongArrayList();
        private final Integer executionNodeId;
        private final int relationIdx;
        private final FetchRelation fetchRelation;

        public NodeBucket(String nodeId,
                          int nodeIdx,
                          int executionNodeId,
                          FetchRelation fetchRelation,
                          int relationIdx) {
            this.nodeId = nodeId;
            this.nodeIdx = nodeIdx;
            this.executionNodeId = executionNodeId;
            this.fetchRelation = fetchRelation;
            this.relationIdx = relationIdx;
        }

        public void add(int cursor, Long docId, @Nullable Row partitionRow, Row row) {
            cursors.add(cursor);
            docIds.add(docId);
            partitionRows.add(partitionRow);
            inputRows.add(new RowN(row.materialize()));
        }

        public int size() {
            return cursors.size();
        }

        public LongArrayList docIds() {
            return docIds;
        }

        public int cursor(int index) {
            return cursors.get(index);
        }

        public Row inputRow(int index) {
            return inputRows.get(index);
        }

        @Nullable
        public Row partitionRow(int idx) {
            return partitionRows.get(idx);
        }

        public FetchRelation fetchRelation() {
            return fetchRelation;
        }
    }

    public static class FetchRelation {

        private final int executionNodeId;
        private final CollectExpression<?> collectDocIdExpression;
        private final List<Symbol> inputSymbols;
        private final List<Input<?>> inputs = new ArrayList<>();
        private final List<ReferenceInfo> partitionedBy;
        private final RowDelegate collectRowDelegate = new RowDelegate();
        private final RowDelegate fetchRowDelegate = new RowDelegate();
        private final RowDelegate partitionRowDelegate = new RowDelegate();
        private final RowInputSymbolVisitor rowInputSymbolVisitor;
        private final RowInputSymbolVisitor.Context collectRowContext;
        private final RowInputSymbolVisitor.Context fetchRowContext;
        private final Row outputRow = new InputRow(inputs);
        private int outputIdx = 0;

        private boolean needInputRow = false;

        public FetchRelation(Functions functions,
                             TableIdent tableIdent,
                             int executionNodeId,
                             CollectExpression<?> collectDocIdExpression,
                             List<Symbol> inputSymbols,
                             List<ReferenceInfo> partitionedBy) {
            this.executionNodeId = executionNodeId;
            this.collectDocIdExpression = collectDocIdExpression;
            this.inputSymbols = inputSymbols;
            this.partitionedBy = partitionedBy;
            this.rowInputSymbolVisitor =  new RowInputSymbolVisitor(functions);

            collectRowContext = new RowInputSymbolVisitor.Context(
                    collectRowDelegate, partitionRowDelegate, partitionedBy, tableIdent);

            fetchRowContext = new RowInputSymbolVisitor.Context(
                    fetchRowDelegate, partitionRowDelegate, partitionedBy, tableIdent);

            // process input symbols (increase input index for every reference)
            for (Symbol symbol : inputSymbols) {
                rowInputSymbolVisitor.process(symbol, collectRowContext);
            }
        }

        public int executionNodeId() {
            return executionNodeId;
        }

        public CollectExpression<?> collectDocIdExpression() {
            return collectDocIdExpression;
        }

        public List<ReferenceInfo> partitionedBy() {
            return partitionedBy;
        }

        public Row outputRow() {
            return outputRow;
        }

        @Nullable
        public Input<?> processOutputSymbol(Symbol symbol) {
            // process output symbol, use different contexts (and so different row delegates)
            // for collect(inputSymbols) & fetch
            Input<?> input;
            if (inputSymbols.contains(symbol)) {
                input = rowInputSymbolVisitor.process(symbol, collectRowContext);
                if (input != null) {
                    needInputRow = true;
                }
            } else {
                input = rowInputSymbolVisitor.process(symbol, fetchRowContext);
            }
            if (input != null) {
                inputs.add(input);
                // rewrite input to output row
                return new RowInputSymbolVisitor.RowInput(outputRow, outputIdx++);
            }
            return null;
        }


        public List<Reference> fetchReferences() {
            return fetchRowContext.fetchReferences();
        }

        public synchronized void processFetchRow(Row row, Row inputRow, Row partitionedByRow) {
            fetchRowDelegate.delegate(row);
            if (needInputRow) {
                collectRowDelegate.delegate(inputRow);
            }
            if (partitionedByRow != null) {
                partitionRowDelegate.delegate(partitionedByRow);
            }
        }
    }

}
