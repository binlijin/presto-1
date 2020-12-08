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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;
import org.apache.druid.segment.filter.SelectorFilter;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public class ConstantStringColumnReader
        implements ColumnReader
{
    private SelectorFilter selectorFilter;
    private Slice value;

    public ConstantStringColumnReader(SelectorFilter selectorFilter)
    {
        this.selectorFilter = requireNonNull(selectorFilter, "selectorDimFilter is null");
        this.value = Slices.utf8Slice(selectorFilter.getValue());
    }

    @Override
    public Block readBlock(Type type, int batchSize)
    {
        checkArgument(type == VARCHAR);
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            type.writeSlice(builder, Slices.copyOf(value));
        }
        return builder.build();
    }
}
