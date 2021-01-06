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
package io.prestosql.druid.column;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.vector.VectorOffset;
import org.apache.druid.segment.vector.VectorValueSelector;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.util.Objects.requireNonNull;

public class DoubleColumnVectorReader
        implements ColumnReader
{
    private static final double PAD_DOUBLE = 0;
    private final VectorOffset vectorOffset;
    private final BaseColumn baseColumn;
    private final VectorValueSelector vectorValueSelector;

    public DoubleColumnVectorReader(VectorOffset vectorOffset, BaseColumn baseColumn)
    {
        this.vectorOffset = requireNonNull(vectorOffset, "vectorOffset is null");
        this.baseColumn = requireNonNull(baseColumn, "baseColumn is null");
        this.vectorValueSelector = this.baseColumn.makeVectorValueSelector(vectorOffset);
    }

    @Override
    public Block readBlock(Type type, int batchSize, boolean filterBatch)
    {
        checkArgument(type == DOUBLE);
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        if (filterBatch) {
            for (int i = 0; i < batchSize; i++) {
                // filter whole batch, no need to get the actual value, return fake data.
                type.writeDouble(builder, PAD_DOUBLE);
            }
            // advance offset.
            vectorOffset.advance();
        }
        else {
            double[] doubleVector = vectorValueSelector.getDoubleVector();
            for (int i = 0; i < batchSize; i++) {
                type.writeDouble(builder, doubleVector[i]);
            }
            vectorOffset.advance();
        }
        return builder.build();
    }
}