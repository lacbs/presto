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
package com.facebook.presto.type;

import com.facebook.presto.bytecode.BytecodeBlock;
import com.facebook.presto.bytecode.BytecodeNode;
import com.facebook.presto.bytecode.Variable;
import com.facebook.presto.bytecode.control.IfStatement;
import com.facebook.presto.bytecode.instruction.Constant;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlFunction;
import com.facebook.presto.operator.scalar.RowFieldReference;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.*;
import com.facebook.presto.sql.gen.Binding;
import com.facebook.presto.sql.gen.BytecodeGeneratorContext;
import com.facebook.presto.sql.gen.BytecodeUtils;
import com.facebook.presto.sql.relational.RowExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.PrintStream;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantFalse;
import static com.facebook.presto.bytecode.expression.BytecodeExpressions.constantTrue;
import static com.facebook.presto.type.RowType.RowField;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public final class RowUtils
{
    private RowUtils()
    {
    }

    public static Block construct(List<Object> values, List<Type> types)
    {
        int n = types.size();
        BlockBuilder blockBuilder =  new InterleavedBlockBuilder(types, new BlockBuilderStatus(), n);
        for (int i = 0; i < n; ++i) {
            TypeJsonUtils.appendToBlockBuilder(types.get(i), values.get(i), blockBuilder);
        }
        return blockBuilder.build();
    }

    public static BytecodeNode generateConstructorBytecode(BytecodeGeneratorContext context, RowType rowType, List<RowExpression> arguments) {
        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType.toString());
        List<Type> typeParameters = rowType.getTypeParameters();
        int n = arguments.size();

        block.comment("BlockBuilderStatus blockBuilderStatus = new BlockBuilderStatus()");
        Variable blockBuilderStatusVar = context.getScope().createTempVariable(BlockBuilderStatus.class);
        block.newObject(BlockBuilderStatus.class);
        block.dup(BlockBuilderStatus.class);
        block.invokeConstructor(BlockBuilderStatus.class);
        block.putVariable(blockBuilderStatusVar);

        block.comment("BlockBuilder blockBuilder = new InterleavedBlockBuilder(types, blockBuilderStatus, n)");
        Variable blockBuilderVar = context.getScope().createTempVariable(BlockBuilder.class);
        block.newObject(InterleavedBlockBuilder.class);
        block.dup(InterleavedBlockBuilder.class);
        Binding typesBinding = context.getCallSiteBinder().bind(typeParameters, List.class);
        block.append(BytecodeUtils.loadConstant(typesBinding));
        block.getVariable(blockBuilderStatusVar);
        block.push(n);
        block.invokeConstructor(InterleavedBlockBuilder.class, List.class, BlockBuilderStatus.class, int.class);
        block.putVariable(blockBuilderVar);

        for (int i = 0; i < n; ++i) {
            Type elementType = typeParameters.get(i);
            Class javaType = elementType.getJavaType();
            if (javaType == void.class) {
                block.getVariable(blockBuilderVar);
                block.invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class);
                block.pop(BlockBuilder.class);
                continue;
            }
            BytecodeBlock appendNullBlock = new BytecodeBlock()
                    .pop(javaType)
                    .getVariable(blockBuilderVar)
                    .invokeInterface(BlockBuilder.class, "appendNull", BlockBuilder.class)
                    .pop(BlockBuilder.class);

            BytecodeBlock appendElementBlock = new BytecodeBlock();
            Binding typeBinding = context.getCallSiteBinder().bind(elementType, Type.class);
            Variable element = context.getScope().createTempVariable(javaType);
            appendElementBlock.putVariable(element);
            appendElementBlock.append(BytecodeUtils.loadConstant(typeBinding));
            appendElementBlock.getVariable(blockBuilderVar);
            appendElementBlock.getVariable(element);

            if (javaType == boolean.class) {
                appendElementBlock.invokeInterface(Type.class, "writeBoolean", void.class, BlockBuilder.class, boolean.class);
                appendElementBlock.pop(void.class);
            }
            else if (javaType == long.class) {
                appendElementBlock.invokeInterface(Type.class, "writeLong", void.class, BlockBuilder.class, long.class);
                appendElementBlock.pop(void.class);
            }
            else if (javaType == double.class) {
                appendElementBlock.invokeInterface(Type.class, "writeDouble", void.class, BlockBuilder.class, double.class);
                appendElementBlock.pop(void.class);
            }
            else if (javaType == Slice.class) {
                appendElementBlock.invokeInterface(Type.class, "writeSlice", void.class, BlockBuilder.class, Slice.class);
                appendElementBlock.pop(void.class);
            }
            else {
                appendElementBlock.invokeInterface(Type.class, "writeObject", void.class, BlockBuilder.class, Object.class);
                appendElementBlock.pop(void.class);
            }

            block.append(context.generate(arguments.get(i)));
            block.append(new IfStatement().condition(context.wasNull()).ifTrue(appendNullBlock).ifFalse(appendElementBlock));
        }
        block.getVariable(blockBuilderVar);
        block.invokeInterface(BlockBuilder.class, "build", Block.class);
        block.append(context.wasNull().set(constantFalse()));
        return block;
    }

    public static Object access(Block row, Type elementType, int index)
    {
        Class javaType = elementType.getJavaType();
        if (javaType == void.class) {
            return null;
        }
        if (javaType == boolean.class) {
            return elementType.getBoolean(row, index - 1);
        }
        if (javaType == long.class) {
            return elementType.getLong(row, index - 1);
        }
        if (javaType == double.class) {
            return elementType.getDouble(row, index - 1);
        }
        if (javaType == Slice.class) {
            return elementType.getSlice(row, index - 1);
        }
        return elementType.getObject(row, index - 1);
    }


    public static BytecodeNode generateAccessorBytecode(BytecodeGeneratorContext context, RowExpression row, Type elementType, int index) {
        BytecodeBlock block = new BytecodeBlock().setDescription("Accessor of " + index + "-th element of a row as " + elementType.toString());
        Class javaType = elementType.getJavaType();

        Variable wasNull = context.wasNull();
        BytecodeBlock nullBlock = new BytecodeBlock().pushJavaDefault(javaType).append(wasNull.set(constantTrue()));
        if (javaType == void.class) {
            return block.append(nullBlock);
        }
        Variable rowVar = context.getScope().createTempVariable(Block.class);
        block.append(context.generate(row));
        block.putVariable(rowVar);

        BytecodeBlock elementIsNullBlock = new BytecodeBlock();
        elementIsNullBlock.getVariable(rowVar);
        elementIsNullBlock.push(index - 1);
        elementIsNullBlock.invokeInterface(Block.class, "isNull", boolean.class, int.class);

        BytecodeBlock getElementBlock = new BytecodeBlock();
        Binding typeBinding = context.getCallSiteBinder().bind(elementType, Type.class);
        getElementBlock.append(BytecodeUtils.loadConstant(typeBinding));
        getElementBlock.append(context.generate(row));
        getElementBlock.push(index - 1);
        if (javaType == boolean.class) {
            getElementBlock.invokeInterface(Type.class, "getBoolean", boolean.class, Block.class, int.class);
        }
        else if (javaType == long.class) {
            getElementBlock.invokeInterface(Type.class, "getLong", long.class, Block.class, int.class);
        }
        else if (javaType == double.class) {
            getElementBlock.invokeInterface(Type.class, "getDouble", double.class, Block.class, int.class);
        }
        else if (javaType == Slice.class) {
            getElementBlock.invokeInterface(Type.class, "getSlice", Slice.class, Block.class, int.class);
        }
        else {
            getElementBlock.invokeInterface(Type.class, "getObject", Object.class, Block.class, int.class);
        }
        getElementBlock.append(wasNull.set(constantFalse()));

        IfStatement getElementOrNull = new IfStatement();
        getElementOrNull.condition(elementIsNullBlock).ifTrue(nullBlock).ifFalse(getElementBlock);
        block.append(new IfStatement().condition(wasNull).ifTrue(nullBlock).ifFalse(getElementOrNull));
        return block;
    }

}
