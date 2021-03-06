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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.crate.action.job.SharedShardContext;
import io.crate.action.job.SharedShardContexts;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.OrderBy;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnhandledServerException;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.Functions;
import io.crate.metadata.PartitionName;
import io.crate.metadata.shard.unassigned.UnassignedShard;
import io.crate.operation.ImplementationSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.ShardCollectService;
import io.crate.operation.collect.collectors.MultiShardScoreDocCollector;
import io.crate.operation.collect.collectors.OrderedDocCollector;
import io.crate.operation.projectors.*;
import io.crate.operation.projectors.sorting.OrderingByPosition;
import io.crate.operation.reference.sys.node.NodeSysExpression;
import io.crate.operation.reference.sys.node.NodeSysReferenceResolver;
import io.crate.planner.RowGranularity;
import io.crate.planner.consumer.OrderByPositionVisitor;
import io.crate.planner.node.dql.CollectPhase;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.shard.IllegalIndexShardStateException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;

@Singleton
public class ShardCollectSource implements CollectSource {

    private static final ESLogger LOGGER = Loggers.getLogger(ShardCollectSource.class);

    private final Settings settings;
    private final IndicesService indicesService;
    private final Functions functions;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final TransportActionProvider transportActionProvider;
    private final BulkRetryCoordinatorPool bulkRetryCoordinatorPool;
    private final UnassignedShardsCollectSource unassignedShardsCollectSource;
    private final NodeSysExpression nodeSysExpression;
    private final ListeningExecutorService executor;

    @Inject
    public ShardCollectSource(Settings settings,
                              IndicesService indicesService,
                              Functions functions,
                              ClusterService clusterService,
                              ThreadPool threadPool,
                              TransportActionProvider transportActionProvider,
                              BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                              UnassignedShardsCollectSource unassignedShardsCollectSource,
                              NodeSysExpression nodeSysExpression) {
        this.settings = settings;
        this.indicesService = indicesService;
        this.functions = functions;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.executor = MoreExecutors.listeningDecorator((ExecutorService) threadPool.executor(ThreadPool.Names.SEARCH));
        this.transportActionProvider = transportActionProvider;
        this.bulkRetryCoordinatorPool = bulkRetryCoordinatorPool;
        this.unassignedShardsCollectSource = unassignedShardsCollectSource;
        this.nodeSysExpression = nodeSysExpression;
    }

    @Override
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        NodeSysReferenceResolver referenceResolver = new NodeSysReferenceResolver(nodeSysExpression);
        ImplementationSymbolVisitor implementationSymbolVisitor = new ImplementationSymbolVisitor(
                referenceResolver,
                functions,
                RowGranularity.NODE
        );
        EvaluatingNormalizer nodeNormalizer = new EvaluatingNormalizer(functions,
                RowGranularity.NODE,
                referenceResolver);
        CollectPhase normalizedPhase = collectPhase.normalize(nodeNormalizer);

        ProjectorFactory projectorFactory = new ProjectionToProjectorVisitor(
                clusterService,
                threadPool,
                settings,
                transportActionProvider,
                bulkRetryCoordinatorPool,
                implementationSymbolVisitor
        );

        String localNodeId = clusterService.localNode().id();
        OrderBy orderBy = normalizedPhase.orderBy();
        if (normalizedPhase.maxRowGranularity() == RowGranularity.DOC && orderBy != null && orderBy.isSorted()) {
            FlatProjectorChain flatProjectorChain;
            if (normalizedPhase.hasProjections()) {
                flatProjectorChain = FlatProjectorChain.withAttachedDownstream(
                        projectorFactory,
                        jobCollectContext.queryPhaseRamAccountingContext(),
                        normalizedPhase.projections(),
                        downstream,
                        collectPhase.jobId()
                );
            } else {
                flatProjectorChain = FlatProjectorChain.withReceivers(ImmutableList.of(downstream));
            }
            return ImmutableList.of(createMultiShardScoreDocCollector(
                    normalizedPhase,
                    flatProjectorChain,
                    jobCollectContext,
                    localNodeId)
            );
        }


        // actual shards might be less if table is partitioned and a partition has been deleted meanwhile
        int maxNumShards = normalizedPhase.routing().numShards(localNodeId);

        ShardProjectorChain projectorChain = ShardProjectorChain.passThroughMerge(
                normalizedPhase.jobId(),
                maxNumShards,
                normalizedPhase.projections(),
                downstream,
                projectorFactory,
                jobCollectContext.queryPhaseRamAccountingContext());

        Map<String, Map<String, List<Integer>>> locations = normalizedPhase.routing().locations();
        if (locations == null) {
            throw new IllegalStateException("locations must not be null");
        }
        final List<CrateCollector> shardCollectors = new ArrayList<>(maxNumShards);

        if (normalizedPhase.maxRowGranularity() == RowGranularity.SHARD) {
            shardCollectors.addAll(
                    getShardCollectors(collectPhase, normalizedPhase, projectorFactory, localNodeId, projectorChain));
        } else {
            Map<String, List<Integer>> indexShards = locations.get(localNodeId);
            if (indexShards != null) {
                shardCollectors.addAll(
                        getDocCollectors(jobCollectContext, normalizedPhase, projectorChain, indexShards));
            }
        }

        if (shardCollectors.isEmpty()) {
            projectorChain.finish();
        } else {
            projectorChain.startProjections(jobCollectContext);
        }
        return shardCollectors;
    }

    private CrateCollector createMultiShardScoreDocCollector(CollectPhase collectPhase,
                                                             FlatProjectorChain flatProjectorChain,
                                                             JobCollectContext jobCollectContext,
                                                             String localNodeId) {

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        assert locations != null : "routing must not be null";

        SharedShardContexts sharedShardContexts = jobCollectContext.sharedShardContexts();
        Map<String, List<Integer>> indexShards = locations.get(localNodeId);
        List<OrderedDocCollector> orderedDocCollectors = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();

            for (Integer shardId : entry.getValue()) {
                SharedShardContext context = sharedShardContexts.getOrCreateContext(new ShardId(indexName, shardId));

                try {
                    Injector shardInjector = context.indexService().shardInjectorSafe(shardId);
                    ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                    orderedDocCollectors.add(shardCollectService.getOrderedCollector(collectPhase, context, jobCollectContext));
                } catch (IndexShardMissingException | CancellationException | IllegalIndexShardStateException e) {
                    throw e;
                } catch (IndexMissingException e) {
                    if (PartitionName.isPartition(indexName)) {
                        break;
                    }
                    throw new TableUnknownException(indexName, e);
                } catch (Throwable t) {
                    throw new UnhandledServerException(t);
                }
            }
        }

        OrderBy orderBy = collectPhase.orderBy();
        assert orderBy != null;
        return new MultiShardScoreDocCollector(
                orderedDocCollectors,
                jobCollectContext.keepAliveListener(),
                OrderingByPosition.rowOrdering(
                        OrderByPositionVisitor.orderByPositions(orderBy.orderBySymbols(), collectPhase.toCollect()),
                        orderBy.reverseFlags(),
                        orderBy.nullsFirst()
                ),
                flatProjectorChain,
                executor
        );
    }

    private Collection<CrateCollector> getDocCollectors(JobCollectContext jobCollectContext,
                                                        CollectPhase collectPhase,
                                                        ShardProjectorChain projectorChain,
                                                        Map<String, List<Integer>> indexShards) {

        List<CrateCollector> crateCollectors = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry : indexShards.entrySet()) {
            String indexName = entry.getKey();
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(indexName);
            } catch (IndexMissingException e) {
                if (PartitionName.isPartition(indexName)) {
                    continue;
                }
                throw new TableUnknownException(entry.getKey(), e);
            }

            for (Integer shardId : entry.getValue()) {
                Injector shardInjector;
                try {
                    shardInjector = indexService.shardInjectorSafe(shardId);
                    ShardCollectService shardCollectService = shardInjector.getInstance(ShardCollectService.class);
                    CrateCollector collector = shardCollectService.getDocCollector(
                            collectPhase,
                            projectorChain,
                            jobCollectContext
                    );
                    crateCollectors.add(collector);
                } catch (IndexShardMissingException | CancellationException | IllegalIndexShardStateException e) {
                    projectorChain.fail(e);
                    throw e;
                } catch (Throwable t) {
                    projectorChain.fail(t);
                    throw new UnhandledServerException(t);
                }
            }
        }
        return crateCollectors;
    }

    private Collection<CrateCollector> getShardCollectors(CollectPhase collectPhase,
                                                          CollectPhase normalizedPhase,
                                                          ProjectorFactory projectorFactory,
                                                          String localNodeId,
                                                          ShardProjectorChain projectorChain) {
        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        assert locations != null : "locations must not be null";
        List<CrateCollector> shardCollectors = new ArrayList<>();
        List<UnassignedShard> unassignedShards = new ArrayList<>();
        Map<String, List<Integer>> indexShardsMap = locations.get(localNodeId);
        for (Map.Entry<String, List<Integer>> indexShards : indexShardsMap.entrySet()) {
            String indexName = indexShards.getKey();
            List<Integer> shards = indexShards.getValue();
            IndexService indexService;
            try {
                indexService = indicesService.indexServiceSafe(indexName);
            } catch (IndexMissingException e) {
                for (Integer shard : shards) {
                    unassignedShards.add(toUnassignedShard(new ShardId(indexName, UnassignedShard.markAssigned(shard))));
                }
                continue;
            }
            for (Integer shard : shards) {
                if (UnassignedShard.isUnassigned(shard)) {
                    unassignedShards.add(toUnassignedShard(new ShardId(indexName, UnassignedShard.markAssigned(shard))));
                    continue;
                }
                try {
                    ShardCollectService shardCollectService =
                            indexService.shardInjectorSafe(shard).getInstance(ShardCollectService.class);
                    shardCollectors.add(
                            shardCollectService.getShardCollector(
                                    normalizedPhase,
                                    projectorChain.newShardDownstreamProjector(projectorFactory)));
                } catch (IndexShardMissingException | IllegalIndexShardStateException e) {
                    unassignedShards.add(toUnassignedShard(new ShardId(indexName, shard)));
                } catch (Throwable t) {
                    projectorChain.fail(t);
                    throw new UnhandledServerException(t);
                }
            }
        }
        if (!unassignedShards.isEmpty()) {
            // since unassigned shards aren't really on any node we use the collectPhase which is NOT normalized here
            // because otherwise if _node was also selected it would contain something which is wrong
            shardCollectors.addAll(unassignedShardsCollectSource.getCollectors(
                    collectPhase, unassignedShards, projectorChain.newShardDownstreamProjector(projectorFactory)));
        }
        return shardCollectors;
    }

    private UnassignedShard toUnassignedShard(ShardId shardId) {
        return new UnassignedShard(shardId, clusterService, false, ShardRoutingState.UNASSIGNED);
    }
}
