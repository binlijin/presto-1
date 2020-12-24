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
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.data.Offset;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class LongColumnReader
        implements ColumnReader
{
    private final Offset offset;
    private final ColumnValueSelector<Long> valueSelector;
    private final DimFilter postFilter;
    private Long constantL;
    private boolean batchAllFilter;

    public LongColumnReader(Offset offset, ColumnValueSelector valueSelector, DimFilter postFilter)
    {
        this.offset = requireNonNull(offset, "offset is null");
        this.valueSelector = requireNonNull(valueSelector, "value selector is null");
        this.postFilter = postFilter;
        if (this.postFilter != null) {
            if (this.postFilter instanceof SelectorDimFilter) {
                SelectorDimFilter selectorDimFilter = (SelectorDimFilter) this.postFilter;
                try {
                    constantL = DimensionHandlerUtils.convertObjectToLong(selectorDimFilter.getValue());
                }
                catch (RuntimeException e) {
                    //TODO Do not throw exception?
                }
            }
        }
        this.batchAllFilter = false;
    }

    @Override
    public Block readBlock(Type type, int batchSize)
    {
        // TODO: use batch value selector
        checkArgument(type == BIGINT);
        batchAllFilter = true;
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            long value = valueSelector.getLong();
            type.writeLong(builder, value);
            offset.increment();
            if (constantL != null && constantL != value) {
                // filter
            }
            else {
                batchAllFilter = false;
            }
        }

        return builder.build();
    }

    public boolean filterBatch()
    {
        return batchAllFilter;
    }
}
