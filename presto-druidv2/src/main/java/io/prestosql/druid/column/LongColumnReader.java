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
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.data.Offset;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static java.util.Objects.requireNonNull;

public class LongColumnReader
        implements ColumnReader
{
    private static final long PAD_LONG = 0;
    private final Offset offset;
    private final ColumnValueSelector<Long> valueSelector;
    private final DimFilter postFilter;
    private Long constantL;
    private long[] longArray;
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
            else if (this.postFilter instanceof InDimFilter) {
                InDimFilter inDimFilter = (InDimFilter) this.postFilter;
                List<Long> values = new ArrayList<>(inDimFilter.getValues().size());
                for (String value : inDimFilter.getValues()) {
                    final Long longValue = DimensionHandlerUtils.convertObjectToLong(value);
                    if (longValue != null) {
                        values.add(longValue);
                    }
                }
                if (!values.isEmpty()) {
                    longArray = new long[values.size()];
                    for (int i = 0; i < values.size(); i++) {
                        longArray[i] = values.get(i);
                    }
                    Arrays.sort(longArray);
                }
            }
        }
        this.batchAllFilter = false;
    }

    @Override
    public Block readBlock(Type type, int batchSize, boolean filterBatch)
    {
        // TODO: use batch value selector
        checkArgument(type == BIGINT);
        boolean hasValue = false;
        BlockBuilder builder = type.createBlockBuilder(null, batchSize);
        for (int i = 0; i < batchSize; i++) {
            long value;
            if (filterBatch) {
                // filter whole batch, no need to get the actual value.
                value = PAD_LONG;
                type.writeLong(builder, value);
            }
            else {
                value = valueSelector.getLong();
                type.writeLong(builder, value);
            }
            offset.increment();
            if (!filterBatch) {
                if (constantL != null && !hasValue) {
                    // check SelectorDimFilter
                    hasValue = checkSelectorDimFilter(value);
                }
                else if (longArray != null && !hasValue) {
                    // check InDimFilter
                    hasValue = checkInDimFilter(value);
                }
            }
        }
        batchAllFilter = false;
        if (constantL != null && !hasValue) {
            batchAllFilter = true;
        }
        else if (longArray != null && !hasValue) {
            batchAllFilter = true;
        }
        return builder.build();
    }

    boolean checkSelectorDimFilter(long value)
    {
        // can not filter
        if (constantL == value) {
            return true;
        }
        return false;
    }

    boolean checkInDimFilter(long value)
    {
        // can not filter
        return Arrays.binarySearch(longArray, value) >= 0;
    }

    @Override
    public boolean hasPostFilter()
    {
        return (constantL != null);
    }

    @Override
    public boolean filterBatch()
    {
        return batchAllFilter;
    }
}
