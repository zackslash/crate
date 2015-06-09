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

package io.crate.planner.node.dql.join;

import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.crate.planner.node.ExecutionNodeVisitor;
import io.crate.planner.node.PlanNodeVisitor;
import io.crate.planner.node.dql.AbstractDQLPlanNode;
import io.crate.planner.node.dql.MergeNode;
import io.crate.planner.projection.Projection;
import io.crate.planner.symbol.Symbols;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class NestedLoopNode extends AbstractDQLPlanNode {

    public static final ExecutionNodeFactory<NestedLoopNode> FACTORY = new ExecutionNodeFactory<NestedLoopNode>() {
        @Override
        public NestedLoopNode create() {
            return new NestedLoopNode();
        }
    };

    private Set<String> executionNodes;

    private List<DataType> inputTypes;

    NestedLoopNode() {}

    @Nullable
    private MergeNode leftMergeNode;

    @Nullable
    private MergeNode rightMergeNode;

    public NestedLoopNode(int executionNodeId,
                          String name,
                          List<Projection> projections,
                          MergeNode leftPreviousNode,
                          MergeNode rightPreviousNode) {
        super(executionNodeId, name, projections);
        inputTypes = new ArrayList<>(leftPreviousNode.outputTypes().size() + rightPreviousNode.outputTypes().size());
        inputTypes.addAll(leftPreviousNode.outputTypes());
        inputTypes.addAll(rightPreviousNode.outputTypes());

        if (projections.isEmpty()) {
            outputTypes = inputTypes;
        } else {
            outputTypes = Symbols.extractTypes(Iterables.getLast(projections).outputs());
        }
    }

    @Override
    public Type type() {
        return Type.NESTED_LOOP;
    }

    @Override
    public Set<String> executionNodes() {
        if (executionNodes == null) {
            return ImmutableSet.of();
        } else {
            return executionNodes;
        }
    }

    public MergeNode leftMergeNode() {
        return leftMergeNode;
    }

    public MergeNode rightMergeNode() {
        return rightMergeNode;
    }

    public void executionNodes(Set<String> executionNodes) {
        this.executionNodes = executionNodes;
    }

    @Override
    public <C, R> R accept(ExecutionNodeVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopNode(this, context);
    }

    @Override
    public <C, R> R accept(PlanNodeVisitor<C, R> visitor, C context) {
        return visitor.visitNestedLoopNode(this, context);
    }

    @Override
    public List<DataType> outputTypes() {
        return outputTypes;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        int numInputTypes = in.readVInt();
        inputTypes = new ArrayList<>(numInputTypes);
        for (int i = 0; i < numInputTypes; i++) {
            inputTypes.add(DataTypes.fromStream(in));
        }

        int numExecutionNodes = in.readVInt();
        if (numExecutionNodes > 0) {
            executionNodes = new HashSet<>(numExecutionNodes);
            for (int i = 0; i < numExecutionNodes; i++) {
                executionNodes.add(in.readString());
            }
        }
        if (in.readBoolean()) {
            leftMergeNode = new MergeNode();
            leftMergeNode.readFrom(in);
        }
        if (in.readBoolean()) {
            rightMergeNode = new MergeNode();
            rightMergeNode.readFrom(in);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeVInt(inputTypes.size());
        for (DataType inputType : inputTypes) {
            DataTypes.toStream(inputType, out);
        }

        if (executionNodes == null) {
            out.writeVInt(0);
        } else {
            out.writeVInt(executionNodes.size());
            for (String node : executionNodes) {
                out.writeString(node);
            }
        }

        if (leftMergeNode == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            leftMergeNode.writeTo(out);
        }
        if (rightMergeNode == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            rightMergeNode.writeTo(out);
        }
    }

    @Override
    public String toString() {
        MoreObjects.ToStringHelper helper = MoreObjects.toStringHelper(this)
                .add("executionNodeId", executionNodeId())
                .add("name", name())
                .add("outputTypes", outputTypes)
                .add("jobId", jobId())
                .add("executionNodes", executionNodes)
                .add("inputTypes", inputTypes);
        return helper.toString();
    }
}
