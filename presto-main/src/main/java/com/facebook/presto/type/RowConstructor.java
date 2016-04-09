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

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.type.RowType.RowField;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.stream.Collectors.toList;

public final class RowConstructor
{
    private RowConstructor()
    {
    }

    public static Block constructRow(List<Object> values, List<Type> types)
    {
        int n = types.size();
        BlockBuilder blockBuilder =  new InterleavedBlockBuilder(types, new BlockBuilderStatus(), n);
        for (int i = 0; i < n; ++i) {
            TypeJsonUtils.appendToBlockBuilder(types.get(i), values.get(i), blockBuilder);
        }
        return blockBuilder.build();
    }


    public static BytecodeNode generateBytecodeBlock(BytecodeGeneratorContext context, RowType rowType, List<RowExpression> arguments) {
        BytecodeBlock block = new BytecodeBlock().setDescription("Constructor for " + rowType.toString());
        List<Type> typeParameters = rowType.getTypeParameters();
        int n = arguments.size();

        Binding typesBinding = context.getCallSiteBinder().bind(typeParameters, List.class);

        block.comment("BlockBuilderStatus blockBuilderStatus = new BlockBuilderStatus()");
        Variable blockBuilderStatusVar = context.getScope().createTempVariable(BlockBuilderStatus.class);
        block.invokeConstructor(BlockBuilderStatus.class);
        block.putVariable(blockBuilderStatusVar);

        block.comment("BlockBuilder blockBuilder = new InterleavedBlockBuilder(types, blockBuilderStatus, n)");
        Variable blockBuilderVar = context.getScope().createTempVariable(BlockBuilder.class);
        block.append(BytecodeUtils.loadConstant(typesBinding));
        block.append(blockBuilderStatusVar);
        block.push(n);
        block.invokeConstructor(InterleavedBlockBuilder.class, List.class, BlockBuilderStatus.class, int.class);
        block.putVariable(blockBuilderVar);

        block.comment("N times: TypeJsonUtils.appendToBlockBuilder(type, argument, blockBuilder)");
        for (int i = 0; i < n; ++i) {
            Binding typeBinding = context.getCallSiteBinder().bind(typeParameters.get(i), Type.class);
            block.append(BytecodeUtils.loadConstant(typeBinding));
            block.append(context.generate(arguments.get(i)));
            block.append(blockBuilderVar);
            block.invokeStatic(TypeJsonUtils.class, "appendToBlockBuilder", Type.class, Object.class, BlockBuilder.class);
        }
        return block;
    }
}
