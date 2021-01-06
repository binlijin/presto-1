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

import com.druid.hdfs.reader.column.HDFSStringDictionaryEncodedColumn;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorOffset;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class StringColumnVectorReader
        implements ColumnReader
{
    private final VectorOffset vectorOffset;
    private final HDFSStringDictionaryEncodedColumn baseColumn;
    private final SingleValueDimensionVectorSelector singleValueDimensionVectorSelector;

    public StringColumnVectorReader(VectorOffset vectorOffset, BaseColumn baseColumn)
    {
        this.vectorOffset = requireNonNull(vectorOffset, "vectorOffset is null");
        this.baseColumn = requireNonNull((HDFSStringDictionaryEncodedColumn) baseColumn, "baseColumn is null");
        this.singleValueDimensionVectorSelector = this.baseColumn.makeSingleValueDimensionVectorSelector(vectorOffset);
    }

    @Override
    public Block readBlock(Type type, int batchSize, boolean filterBatch)
    {
        checkArgument(type == VARCHAR);
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        if (filterBatch) {
            for (int i = 0; i < batchSize; i++) {
                // filter whole batch, no need to get the actual value, return fake data.
                builder.appendNull();
            }
        }
        else {
            int[] rowVector = singleValueDimensionVectorSelector.getRowVector();
            for (int i = 0; i < batchSize; i++) {
                byte[] object = baseColumn.lookupObjectByte(rowVector[i]);
                if (object != null && object.length > 0) {
                    type.writeSlice(builder, Slices.wrappedBuffer(object, 0, object.length));
                }
                else {
                    builder.appendNull();
                }
            }
            vectorOffset.advance();
        }

        return builder.build();
    }
}
