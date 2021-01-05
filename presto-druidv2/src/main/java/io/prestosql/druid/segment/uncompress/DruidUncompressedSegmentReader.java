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
package io.prestosql.druid.segment.uncompress;

import com.druid.hdfs.reader.HDFSSimpleQueryableIndex;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.prestosql.druid.DruidColumnHandle;
import io.prestosql.druid.column.BitmapReadableOffset;
import io.prestosql.druid.column.ColumnReader;
import io.prestosql.druid.column.SimpleReadableOffset;
import io.prestosql.druid.segment.SegmentReader;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.type.Type;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.query.BitmapResultFactory;
import org.apache.druid.query.DefaultBitmapResultFactory;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.segment.ColumnSelectorBitmapIndexSelector;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.SelectorFilter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static io.prestosql.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static io.prestosql.druid.column.ColumnReader.createColumnReader;
import static io.prestosql.druid.column.ColumnReader.createConstantsColumnReader;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class DruidUncompressedSegmentReader
        implements SegmentReader
{
    private static final Logger LOG = Logger.get(DruidUncompressedSegmentReader.class);

    private final Map<String, ColumnReader> columnValueSelectors;
    private final int totalRowCount;
    private final DimFilter filter;
    private final int maxBatchSize;
    private final Path segmentPath;

    private QueryableIndex queryableIndex;
    private long currentPosition;
    private int currentBatchSize;
    private Map<String, SelectorFilter> constantFields = new ConcurrentHashMap<>();
    private Map<String, DimFilter> postFilterFields = new ConcurrentHashMap<>();

    public DruidUncompressedSegmentReader(
            FileSystem fileSystem,
            Path segmentPath,
            int maxBatchSize,
            List<ColumnHandle> columns,
            DimFilter filter,
            long limit)
    {
        try {
            this.segmentPath = segmentPath;
            this.maxBatchSize = maxBatchSize;
            this.filter = filter;
            V9UncompressedSegmentIndexSource uncompressedSegmentIndexSource =
                    new V9UncompressedSegmentIndexSource(fileSystem, segmentPath);
            queryableIndex = uncompressedSegmentIndexSource.loadIndex(columns);

            ImmutableBitmap filterBitmap = analyzeFilter(this.filter);
            if (filterBitmap != null) {
                totalRowCount = (int) Long.min(filterBitmap.size(), Long.min(queryableIndex.getNumRows(), limit));
                if (LOG.isDebugEnabled()) {
                    LOG.debug("DruidSegmentReader for " + segmentPath + ", select " + filterBitmap.size() + "/"
                            + queryableIndex.getNumRows() + " rows ");
                }
            }
            else {
                totalRowCount = (int) Long.min(queryableIndex.getNumRows(), limit);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("DruidSegmentReader for " + segmentPath + ", totalRowCount = " + totalRowCount + ", limit = " + limit
                        + ", filter = " + this.filter + ", maxBatchSize = " + maxBatchSize);
            }
            ImmutableMap.Builder<String, ColumnReader> selectorsBuilder = ImmutableMap.builder();
            for (ColumnHandle column : columns) {
                Offset offset = null;
                if (filterBitmap != null) {
                    offset = new BitmapReadableOffset(filterBitmap);
                }
                else {
                    offset = new SimpleReadableOffset(totalRowCount);
                }
                DruidColumnHandle druidColumn = (DruidColumnHandle) column;
                String columnName = druidColumn.getColumnName();
                Type type = druidColumn.getColumnType();
                //LOG.debug("Read columnName = " + columnName + ", type " + type);
                if (constantFields.containsKey(columnName)) {
                    // Do not need to read this column.
                    SelectorFilter selectorFilter = constantFields.get(columnName);
                    selectorsBuilder.put(columnName, createConstantsColumnReader(type, selectorFilter));
                    //LOG.debug("Constants columnName = " + columnName + ", value = " + selectorFilter.getValue() + ", type " + type);
                }
                else {
                    BaseColumn baseColumn = queryableIndex.getColumnHolder(columnName).getColumn();
                    //ColumnValueSelector<?> valueSelector = baseColumn.makeColumnValueSelector(offset);
                    selectorsBuilder.put(columnName, createColumnReader(type, baseColumn, offset, postFilterFields.get(columnName)));
                }
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
        currentBatchSize = toIntExact(min(maxBatchSize, totalRowCount - currentPosition));
        currentPosition += currentBatchSize;
        return currentBatchSize;
    }

    @Override
    public Block readBlock(Type type, String columnName, boolean filterBatch)
    {
        return columnValueSelectors.get(columnName).readBlock(type, currentBatchSize, filterBatch);
    }

    public ColumnReader getColumnReader(String columnName)
    {
        return columnValueSelectors.get(columnName);
    }

    public long getReadTimeNanos()
    {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug(segmentPath + " ReadTimeMs = "
//                    + ((HDFSSimpleQueryableIndex) queryableIndex).getReadTimeNanos() / 1000000
//                    + " ms.");
//        }
        return ((HDFSSimpleQueryableIndex) queryableIndex).getReadTimeNanos();
    }

    public Path getSegmentPath()
    {
        return segmentPath;
    }

    public ImmutableBitmap analyzeFilter(@Nullable final DimFilter dimFilter)
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
        if (dimFilter == null) {
            preFilters = Collections.emptySet();
        }
        else {
            preFilters = new HashSet<>();

            if (dimFilter instanceof DimFilter) {
                // If we get an AndFilter, we can split the subfilters across both filtering stages
                for (DimFilter field : ((AndDimFilter) dimFilter).getFields()) {
                    Filter subfilter = field.toFilter();
                    // only string typed dimensions support bitmap index
                    // https://druid.apache.org/docs/latest/ingestion/index.html#dimensionsspec
                    if (subfilter.supportsBitmapIndex(indexSelector) && subfilter
                            .shouldUseBitmapIndex(indexSelector)) {
                        preFilters.add(subfilter);
                        if (subfilter instanceof SelectorFilter) {
                            SelectorFilter selectorFilter = (SelectorFilter) subfilter;
                            constantFields.put(selectorFilter.getDimension(), selectorFilter);
                        }
                    }
                    else {
                        //
                        postFilters.add(subfilter);
                        if (subfilter instanceof SelectorFilter) {
                            SelectorFilter selectorFilter = (SelectorFilter) subfilter;
                            postFilterFields.put(selectorFilter.getDimension(), field);
                        }
                        if (field instanceof InDimFilter) {
                            InDimFilter inDimFilter = (InDimFilter) field;
                            postFilterFields.put(inDimFilter.getDimension(), field);
                        }
                        if (field instanceof BoundDimFilter) {
                            //TODO
                        }
                    }
                }
            }
            else {
                Filter filter = dimFilter.toFilter();
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
    public void close() throws IOException
    {
//        if (LOG.isDebugEnabled()) {
//            LOG.debug(segmentPath + " ReadTimeMs = "
//                    + ((HDFSSimpleQueryableIndex) queryableIndex).getReadTimeNanos() / 1000000
//                    + " ms.");
//        }
        //TODO
        queryableIndex.close();
    }
}
