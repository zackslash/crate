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

package io.crate.operator.aggregation;

import io.crate.metadata.Scalar;
import io.crate.operator.Input;

public class FunctionCollectExpression<ReturnType> extends CollectExpression<ReturnType> {

    private final Input<?>[] childInputs;
    private Scalar<ReturnType> functionImplementation;

    public FunctionCollectExpression(Scalar<ReturnType> functionImplementation, Input<?>[] childInputs) {
        this.functionImplementation = functionImplementation;
        this.childInputs = childInputs;
    }

    @Override
    public boolean setNextRow(Object... args) {
        for (Input<?> childInput : childInputs) {
            if (childInput instanceof CollectExpression<?>) {
                ((CollectExpression) childInput).setNextRow(args);
            }
        }
        return true;
    }

    @Override
    public ReturnType value() {
        Object[] argumentValues = new Object[childInputs.length];
        int i = 0;
        for (Input<?> childInput : childInputs) {
            argumentValues[i++] = childInput.value();
        }
        return functionImplementation.evaluate(argumentValues);
    }
}
