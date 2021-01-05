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
import org.apache.druid.segment.ColumnValueSelector;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class LongColumnReader
        implements ColumnReader
{
    private final ColumnValueSelector<Long> valueSelector;

    public LongColumnReader(ColumnValueSelector valueSelector)
    {
        this.valueSelector = requireNonNull(valueSelector, "value selector is null");
    }

    @Override
    public Block readBlock(Type type, int batchSize, boolean filterBatch)
    {
        // TODO: use batch value selector
        checkArgument(type == BIGINT);
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            type.writeLong(builder, valueSelector.getLong());
        }

        return builder.build();
    }
}
