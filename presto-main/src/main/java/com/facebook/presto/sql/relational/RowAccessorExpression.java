/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.type.RowType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class RowAccessorExpression
        extends RowExpression
{
    private final RowExpression row;
    private final Type elementType;
    private final int index;

    public RowAccessorExpression(RowExpression row, Type elementType, int index)
    {
        requireNonNull(row, "row is null");
        requireNonNull(elementType, "elementType is null");

        this.row = row;
        this.elementType = elementType;
        this.index = index;
    }

    public RowAccessorExpression(RowExpression row, Expression index)
    {
        requireNonNull(row, "row is null");
        requireNonNull(index, "index is null");

        this.row = row;
        this.index = (int) ((LongLiteral) index).getValue();
        this.elementType = ((RowType) row.getType()).getTypeParameters().get(this.index - 1);
    }

    @Override
    public Type getType()
    {
        return elementType;
    }

    public RowExpression getRow()
    {
        return row;
    }

    public int getIndex()
    {
        return index;
    }

    @Override
    public String toString() { return row.toString() + "[" + index + "]";}

    @Override
    public int hashCode()
    {
        return Objects.hash(row, elementType, index);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RowAccessorExpression other = (RowAccessorExpression) obj;
        return Objects.equals(this.row, other.row) && Objects.equals(this.elementType, other.elementType) && this.index == other.index;
    }

    @Override
    public <C, R> R accept(RowExpressionVisitor<C, R> visitor, C context)
    {
        return visitor.visitRowAccessor(this, context);
    }
}
