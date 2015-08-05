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

package io.crate.planner.fetch;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.symbol.*;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.*;

public class FetchPushDown {

    private static final LinkedHashMap<Symbol, InputColumn> EMPTY_INPUTS = new LinkedHashMap<>(0);
    private static final ReferencesToFetchVisitor referencesToFetchVisitor = new ReferencesToFetchVisitor();
    private static final FetchRequiredVisitor fetchRequiredVisitor = new FetchRequiredVisitor();

    private static class Context {

        private final LinkedHashMap<Symbol, InputColumn> requiredInSub;

        private Context(LinkedHashMap<Symbol, InputColumn> sub) {
            requiredInSub = sub;
        }

        public Set<Symbol> requiredSymbols() {
            if (requiredInSub != null) {
                return requiredInSub.keySet();
            }
            return ImmutableSet.of();
        }

        @Nullable
        public InputColumn queryColumn(Symbol symbol) {
            if (requiredInSub != null) {
                return requiredInSub.get(symbol);
            }
            return null;
        }

        public Symbol allocateReference(Reference symbol) {
            InputColumn col = queryColumn(symbol);
            if (col != null) {
                return col;
            } else {
                return new FetchReference(new InputColumn(0, DataTypes.LONG), symbol);
            }
        }
    }

    @Nullable
    public static QuerySpec pushDown(QuerySpec querySpec, TableIdent tableIdent) {

        assert querySpec.groupBy() == null && querySpec.having() == null && !querySpec.hasAggregates();

        LinkedHashMap<Symbol, InputColumn> requiredInQuery;
        OrderBy orderBy = querySpec.orderBy();

        if (orderBy != null) {
            requiredInQuery = new LinkedHashMap<>(orderBy.orderBySymbols().size());
            int i = 1;
            for (Symbol symbol : orderBy.orderBySymbols()) {
                if (!requiredInQuery.containsKey(symbol)) {
                    requiredInQuery.put(symbol, new InputColumn(i++, symbol.valueType()));
                }
            }
        } else {
            requiredInQuery = EMPTY_INPUTS;
        }
        Context context = new Context(requiredInQuery);

        boolean fetchRequired = fetchRequiredVisitor.process(querySpec.outputs(), context.requiredSymbols());

        if (!fetchRequired) return null;

        List<Symbol> newOutputs = new ArrayList<>(querySpec.outputs());
        int i = 0;
        for (Symbol output : newOutputs) {
            newOutputs.set(i, referencesToFetchVisitor.process(output, context));
            i++;
        }
        querySpec.outputs(newOutputs);

        // build the subquery

        QuerySpec sub = new QuerySpec();
        Reference docIdReference = new Reference(DocSysColumns.forTable(tableIdent, DocSysColumns.DOCID));

        if (orderBy != null) {
            sub.orderBy(querySpec.orderBy());
            querySpec.orderBy(new OrderBy(new ArrayList<Symbol>(context.requiredInSub.values()),
                    orderBy.reverseFlags(), orderBy.nullsFirst()));
            ArrayList<Symbol> outputs = new ArrayList<>(orderBy.orderBySymbols().size() + 1);
            outputs.add(docIdReference);
            outputs.addAll(context.requiredInSub.keySet());
            sub.outputs(outputs);
        } else {
            sub.outputs(ImmutableList.<Symbol>of(docIdReference));
        }

        // push down the where clause
        sub.where(querySpec.where());
        querySpec.where(null);

        // push down offset and limit
        sub.limit(querySpec.limit());
        if (querySpec.offset() > 0) {
            sub.offset(querySpec.offset());
            querySpec.offset(0);
        }

        return sub;
    }

    private static class FetchRequiredVisitor extends SymbolVisitor<Collection<Symbol>, Boolean> {

        public boolean process(List<Symbol> symbols, Collection<Symbol> requiredInQuery) {
            for (Symbol symbol : symbols) {
                if (process(symbol, requiredInQuery)) return true;
            }
            return false;
        }

        @Override
        public Boolean visitReference(Reference symbol, Collection<Symbol> requiredInQuery) {
            return !requiredInQuery.contains(symbol);
        }

        @Override
        public Boolean visitDynamicReference(DynamicReference symbol, Collection<Symbol> requiredInQuery) {
            return visitReference(symbol, requiredInQuery);
        }

        @Override
        protected Boolean visitSymbol(Symbol symbol, Collection<Symbol> requiredInQuery) {
            return false;
        }

        @Override
        public Boolean visitAggregation(Aggregation symbol, Collection<Symbol> requiredInQuery) {
            return !requiredInQuery.contains(symbol) && process(symbol.inputs(), requiredInQuery);
        }

        @Override
        public Boolean visitFunction(Function symbol, Collection<Symbol> requiredInQuery) {
            return !requiredInQuery.contains(symbol) && process(symbol.arguments(), requiredInQuery);
        }
    }

    private static class ReferencesToFetchVisitor extends SymbolVisitor<Context, Symbol> {

        public void process(List<Symbol> symbols, Context context) {
            for (int i = 0; i < symbols.size(); i++) {
                symbols.set(i, process(symbols.get(i), context));
            }
        }

        @Override
        protected Symbol visitSymbol(Symbol symbol, Context context) {
            InputColumn col = context.queryColumn(symbol);
            if (col != null) {
                return col;
            }
            return symbol;
        }

        @Override
        public Symbol visitInputColumn(InputColumn inputColumn, Context context) {
            return inputColumn;
        }

        @Override
        public Symbol visitAggregation(Aggregation aggregation, Context context) {
            InputColumn col = context.queryColumn(aggregation);
            if (col != null) {
                return col;
            }
            process(aggregation.inputs(), context);
            return aggregation;
        }

        @Override
        public Symbol visitReference(Reference symbol, Context context) {
            return context.allocateReference(symbol);
        }

        @Override
        public Symbol visitDynamicReference(DynamicReference symbol, Context context) {
            return visitReference(symbol, context);
        }

        @Override
        public Symbol visitFunction(Function function, Context context) {
            InputColumn col = context.queryColumn(function);
            if (col != null) {
                return col;
            }
            process(function.arguments(), context);
            return function;
        }

    }
}
