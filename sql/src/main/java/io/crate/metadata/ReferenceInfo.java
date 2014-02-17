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

package io.crate.metadata;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import io.crate.planner.RowGranularity;
import org.cratedb.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class ReferenceInfo implements Comparable<ReferenceInfo>, Streamable {

    public static class Builder {
        private ReferenceIdent ident;
        private DataType type;
        private ObjectType objectType = ObjectType.DYNAMIC;
        private RowGranularity granularity;
        private ImmutableList.Builder<ReferenceInfo> nestedColumnsBuilder = new ImmutableList.Builder<>();

        public Builder type(DataType type) {
            this.type = type;
            return this;
        }

        public Builder granularity(RowGranularity rowGranularity) {
            this.granularity = rowGranularity;
            return this;
        }

        public Builder ident(ReferenceIdent ident) {
            this.ident = ident;
            return this;
        }

        public Builder ident(TableIdent table, ColumnIdent column) {
            this.ident = new ReferenceIdent(table, column);
            return this;
        }

        public ReferenceIdent ident() {
            return ident;
        }

        public Builder addNestedColumn(ReferenceInfo column) {
            nestedColumnsBuilder.add(column);
            return this;
        }

        public Builder objectType(ObjectType objectType) {
            this.objectType = objectType;
            return this;
        }

        public Builder objectType(boolean dynamic, boolean strict) {
            if (dynamic) {
                this.objectType = ObjectType.DYNAMIC;
            } else if (!strict) {
                this.objectType = ObjectType.IGNORED;
            } else {
                this.objectType = ObjectType.STRICT;
            }
            return this;
        }

        public ReferenceInfo build() {
            Preconditions.checkNotNull(ident);
            Preconditions.checkNotNull(granularity);
            Preconditions.checkNotNull(type);
            return new ReferenceInfo(ident, granularity, type, nestedColumnsBuilder.build(), objectType);
        }
    }

    public static enum ObjectType {
        DYNAMIC,
        STRICT,
        IGNORED
    }

    private ReferenceIdent ident;
    private DataType type;
    private ObjectType objectType = ObjectType.DYNAMIC;
    private RowGranularity granularity;
    private List<ReferenceInfo> nestedColumns = ImmutableList.of();

    public ReferenceInfo() {

    }

    public ReferenceInfo(ReferenceIdent ident, RowGranularity granularity, DataType type) {
        this(ident, granularity, type, null, ObjectType.DYNAMIC);
    }

    public ReferenceInfo(ReferenceIdent ident,
                         RowGranularity granularity,
                         DataType type,
                         @Nullable List<ReferenceInfo> nestedColumns) {
        this(ident, granularity, type, nestedColumns, ObjectType.DYNAMIC);
    }

    public ReferenceInfo(ReferenceIdent ident,
                         RowGranularity granularity,
                         DataType type,
                         @Nullable List<ReferenceInfo> nestedColumns,
                         ObjectType objectType) {
        this.ident = ident;
        this.type = type;
        this.granularity = granularity;
        this.nestedColumns = Objects.firstNonNull(nestedColumns, ImmutableList.<ReferenceInfo>of());
        this.objectType = objectType;
    }

    public ReferenceIdent ident() {
        return ident;
    }

    public DataType type() {
        return type;
    }

    public RowGranularity granularity() {
        return granularity;
    }

    public boolean hasNestedColumns() {
        return type == DataType.OBJECT && nestedColumns.size() > 0;
    }

    public List<ReferenceInfo> nestedColumns() {
        return nestedColumns;
    }

    public ObjectType objectType() {
        return objectType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReferenceInfo that = (ReferenceInfo) o;

        if (granularity != that.granularity) return false;
        if (ident != null ? !ident.equals(that.ident) : that.ident != null) return false;
        if (nestedColumns != null ? !nestedColumns.equals(that.nestedColumns) : that.nestedColumns != null)
            return false;
        if (objectType.ordinal() != that.objectType.ordinal()) { return false; }
        if (type != that.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(granularity, ident, type, nestedColumns, objectType);
    }

    @Override
    public String toString() {
        Objects.ToStringHelper helper = Objects.toStringHelper(this)
                .add("granularity", granularity)
                .add("ident", ident)
                .add("type", type);
        if (type == DataType.OBJECT) {
            helper.add("object type", objectType.name());
        }
        if (nestedColumns.size() > 0) {
            helper.add("nested columns", nestedColumns);
        }
        return helper.toString();
    }

    @Override
    public int compareTo(ReferenceInfo o) {
        return ComparisonChain.start()
                .compare(granularity, o.granularity)
                .compare(ident, o.ident)
                .compare(type, o.type)
                .compare(objectType.ordinal(), o.objectType.ordinal())
                .compare(nestedColumns, o.nestedColumns, new Comparator<List<ReferenceInfo>>() {
                    @Override
                    public int compare(List<ReferenceInfo> o1, List<ReferenceInfo> o2) {
                        Iterator<ReferenceInfo> o2It = o2.iterator();
                        for (ReferenceInfo info : o1) {
                            if (!o2It.hasNext()) {
                                return 1;
                            }
                            int cmp = info.compareTo(o2It.next());
                            if (cmp != 0) { return cmp; }
                        }
                        if (o2It.hasNext()) { return -1; }
                        else { return 0; }
                    }
                })
                .result();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        ident = new ReferenceIdent();
        ident.readFrom(in);
        type = DataType.fromStream(in);
        granularity = RowGranularity.fromStream(in);

        int numNestedColumns = in.readVInt();
        nestedColumns = new ArrayList<>(numNestedColumns);
        while(numNestedColumns > 0) {
            ReferenceInfo nestedInfo = new ReferenceInfo();
            nestedInfo.readFrom(in);
            nestedColumns.add(nestedInfo);
            numNestedColumns--;
        }
        objectType = ObjectType.values()[in.readVInt()];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        ident.writeTo(out);
        DataType.toStream(type, out);
        RowGranularity.toStream(granularity, out);

        out.writeVInt(nestedColumns.size());
        for (ReferenceInfo nestedInfo : nestedColumns) {
            nestedInfo.writeTo(out);
        }
        out.writeVInt(objectType.ordinal());
    }

    public static Builder builder() {
        return new Builder();
    }
}
