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

import com.druid.hdfs.reader.column.ColumnValueReader;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.data.Offset;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class StringColumnReader
        implements ColumnReader
{
    private final Offset offset;
    private final ColumnValueSelector<String> valueSelector;
    private final boolean optimize;
    private ColumnValueReader columnValueReader;

    public StringColumnReader(Offset offset, ColumnValueSelector valueSelector)
    {
        this.offset = requireNonNull(offset, "offset is null");
        this.valueSelector = requireNonNull(valueSelector, "value selector is null");
        this.optimize = valueSelector instanceof ColumnValueReader;
        if (optimize) {
            columnValueReader = (ColumnValueReader) valueSelector;
        }
    }

    @Override
    public Block readBlock(Type type, int batchSize)
    {
        checkArgument(type == VARCHAR);
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (optimize) {
                byte[] object = columnValueReader.getObjectByte();
                if (object != null && object.length > 0) {
                    type.writeSlice(builder, Slices.wrappedBuffer(object, 0, object.length));
                }
                else {
                    builder.appendNull();
                }
            }
            else {
                String value = String.valueOf(valueSelector.getObject());
                if (value != null) {
                    type.writeSlice(builder, Slices.utf8Slice(value));
                }
                else {
                    builder.appendNull();
                }
            }
            offset.increment();
        }

        return builder.build();
    }
}
