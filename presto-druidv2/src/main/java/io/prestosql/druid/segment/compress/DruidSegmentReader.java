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
package io.prestosql.druid.segment.compress;

import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.druid.DruidColumnHandle;
import io.prestosql.druid.column.BitmapReadableOffset;
import io.prestosql.druid.column.ColumnReader;
import io.prestosql.druid.column.SimpleReadableOffset;
import io.prestosql.druid.segment.SegmentIndexSource;
import io.prestosql.druid.segment.SegmentReader;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.ColumnSelectorBitmapIndexSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.Filters;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.prestosql.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static io.prestosql.druid.column.ColumnReader.createColumnReader;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class DruidSegmentReader
        implements SegmentReader
{
    private static final Logger LOG = Logger.get(DruidSegmentReader.class);

    private static final int BATCH_SIZE = 1024;

    private final Map<String, ColumnReader> columnValueSelectors;
    private final long totalRowCount;
    private final DimFilter filter;

    private QueryableIndex queryableIndex;
    private long currentPosition;
    private int currentBatchSize;

    public DruidSegmentReader(SegmentIndexSource segmentIndexSource, List<ColumnHandle> columns,
            DimFilter filter, long limit)
    {
        try {
            queryableIndex = segmentIndexSource.loadIndex(columns);
            this.filter = filter;
            ImmutableBitmap filterBitmap = analyzeFilter(Filters.toFilter(filter));
            if (filterBitmap != null) {
                totalRowCount =
                        Long.min(filterBitmap.size(), Long.min(queryableIndex.getNumRows(), limit));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("DruidSegmentReader select " + filterBitmap.size() + "/"
                            + queryableIndex.getNumRows() + " rows ");
                }
            }
            else {
                totalRowCount = Long.min(queryableIndex.getNumRows(), limit);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "DruidSegmentReader totalRowCount = " + totalRowCount + ", limit = " + limit
                                + ", filter = " + filter);
            }
            ImmutableMap.Builder<String, ColumnReader> selectorsBuilder = ImmutableMap.builder();
            for (ColumnHandle column : columns) {
                Offset offset = null;
                if (filterBitmap != null) {
                    offset = new BitmapReadableOffset(filterBitmap);
                }
                else {
                    offset = new SimpleReadableOffset((int) totalRowCount);
                }
                DruidColumnHandle druidColumn = (DruidColumnHandle) column;
                String columnName = druidColumn.getColumnName();
                Type type = druidColumn.getColumnType();
                BaseColumn baseColumn = queryableIndex.getColumnHolder(columnName).getColumn();
                ColumnValueSelector<?> valueSelector = baseColumn.makeColumnValueSelector(offset);
                selectorsBuilder.put(columnName, createColumnReader(type, valueSelector));
            }
            columnValueSelectors = selectorsBuilder.build();
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, "failed to load druid segment");
        }
    }

    @Override
    public int nextBatch()
    {
        // TODO: dynamic batch sizing
        currentBatchSize = toIntExact(min(BATCH_SIZE, totalRowCount - currentPosition));
        currentPosition += currentBatchSize;
        return currentBatchSize;
    }

    public ImmutableBitmap analyzeFilter(@Nullable final Filter filter)
    {
        ImmutableBitmap filterBitmap = null;

        final int totalRows = queryableIndex.getNumRows();
        ColumnSelectorBitmapIndexSelector indexSelector = new ColumnSelectorBitmapIndexSelector(
                queryableIndex.getBitmapFactoryForDimensions(),
                VirtualColumns.EMPTY,
                queryableIndex);

        final Set<Filter> preFilters;
        final List<Filter> postFilters = new ArrayList<>();
        int preFilteredRows = totalRows;
        if (filter == null) {
            preFilters = Collections.emptySet();
        }
        else {
            preFilters = new HashSet<>();

            if (filter instanceof AndFilter) {
                // If we get an AndFilter, we can split the subfilters across both filtering stages
                for (Filter subfilter : ((AndFilter) filter).getFilters()) {
                    if (subfilter.supportsBitmapIndex(indexSelector) && subfilter
                            .shouldUseBitmapIndex(indexSelector)) {
                        preFilters.add(subfilter);
                    }
                    else {
                        postFilters.add(subfilter);
                    }
                }
            }
            else {
                // If we get an OrFilter or a single filter, handle the filter in one stage
                if (filter.supportsBitmapIndex(indexSelector) && filter
                        .shouldUseBitmapIndex(indexSelector)) {
                    preFilters.add(filter);
                }
                else {
                    postFilters.add(filter);
                }
            }
        }
        if (preFilters.isEmpty()) {
            filterBitmap = null;
        }
        else {
            BitmapResultFactory<?> bitmapResultFactory =
                    new DefaultBitmapResultFactory(indexSelector.getBitmapFactory());
            filterBitmap = AndFilter.getBitmapIndex(indexSelector, bitmapResultFactory, preFilters);
        }
        return filterBitmap;
    }

    @Override
    public Block readBlock(Type type, String columnName, boolean filterBatch)
    {
        return columnValueSelectors.get(columnName).readBlock(type, currentBatchSize, filterBatch);
    }

    @Override
    public void close() throws IOException
    {
        //TODO
        queryableIndex.close();
    }
}
