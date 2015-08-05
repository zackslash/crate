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

package io.crate.planner.symbol;

import com.google.common.base.MoreObjects;
import io.crate.metadata.ReferenceIdent;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

public class FetchReference extends Symbol {

    private DataType dataType;
    private Symbol docId;
    private ReferenceIdent referenceIdent;

    public static final SymbolFactory FACTORY = new SymbolFactory() {
        @Override
        public Symbol newInstance() {
            return new FetchReference();
        }
    };

    public FetchReference(Symbol docId, Reference reference) {
        this.docId = docId;
        this.referenceIdent = reference.ident();
        this.dataType = reference.valueType();
    }

    public FetchReference() {

    }

    public Symbol docId() {
        return docId;
    }

    public ReferenceIdent referenceIdent() {
        return referenceIdent;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.FETCH_REFERENCE;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitFetchReference(this, context);
    }

    @Override
    public DataType valueType() {
        return dataType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        docId = Symbol.fromStream(in);
        referenceIdent = new ReferenceIdent();
        referenceIdent.readFrom(in);
        dataType = DataTypes.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Symbol.toStream(docId, out);
        referenceIdent.writeTo(out);
        DataTypes.toStream(dataType, out);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("docId", docId)
                .add("referenceIdent", referenceIdent)
                .add("dataType", dataType)
                .toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FetchReference reference = (FetchReference) o;
        return Objects.equals(docId, reference.docId) &&
                Objects.equals(referenceIdent, reference.referenceIdent);
    }

    @Override
    public int hashCode() {
        return (referenceIdent == null) ? 0 : referenceIdent.hashCode();
    }
}
