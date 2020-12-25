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

import io.airlift.log.Logger;
import io.prestosql.druid.DruidColumnHandle;
import io.prestosql.druid.column.ColumnReader;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class DruidUncompressedSegmentPageSource
        implements ConnectorPageSource
{
    private static final Logger LOG = Logger.get(DruidUncompressedSegmentPageSource.class);

    private final List<ColumnHandle> columns;
    private final DruidUncompressedSegmentReader segmentReader;

    private int batchId;
    private boolean closed;
    private long completedBytes;
    private long completedPositions;
    private long buildPageTime;
    private long compressTime;
    private long compressNum;
    private long startTime;

    public DruidUncompressedSegmentPageSource(
            List<ColumnHandle> columns,
            DruidUncompressedSegmentReader segmentReader)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.segmentReader = requireNonNull(segmentReader, "segmentReader is null");
        this.buildPageTime = 0L;
        this.compressTime = 0L;
        this.compressNum = 0L;
        this.startTime = System.nanoTime();
    }

    @Override public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override public long getReadTimeNanos()
    {
        return segmentReader.getReadTimeNanos();
    }

    @Override public boolean isFinished()
    {
        return closed;
    }

    @Override public Page getNextPage()
    {
        long start = System.nanoTime();
        batchId++;
        int batchSize = segmentReader.nextBatch();
        if (batchSize <= 0) {
            close();
            return null;
        }
        Block[] blocks = new Block[columns.size()];
        // First get column that has post filters.
        boolean filterBatch = false;
        for (int i = 0; i < blocks.length; i++) {
            DruidColumnHandle columnHandle = (DruidColumnHandle) columns.get(i);
            ColumnReader columnReader = segmentReader.getColumnReader(columnHandle.getColumnName());
            if (columnReader.hasPostFilter()) {
                blocks[i] = columnReader.readBlock(columnHandle.getColumnType(), batchSize, false);
                if (columnReader.filterBatch()) {
                    filterBatch = true;
                }
                //System.out.println("column = " + columnHandle.getColumnName() + ", type " + columnHandle.getColumnType());
                //System.out.println("batchSize " + batchSize + ", filter batch = " + columnReader.filterBatch());
            }
        }
        for (int i = 0; i < blocks.length; i++) {
            DruidColumnHandle columnHandle = (DruidColumnHandle) columns.get(i);
            ColumnReader columnReader = segmentReader.getColumnReader(columnHandle.getColumnName());
            if (!columnReader.hasPostFilter()) {
                blocks[i] = columnReader.readBlock(columnHandle.getColumnType(), batchSize, filterBatch);
            }
            //blocks[i] = new LazyBlock(batchSize, new SegmentBlockLoader(columnHandle.getColumnType(), columnHandle.getColumnName()));
        }
        Page page = new Page(batchSize, blocks);
        completedBytes += page.getSizeInBytes();
        completedPositions += page.getPositionCount();
        buildPageTime += (System.nanoTime() - start);
        return page;
    }

    @Override public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override public void close()
    {
        if (closed) {
            return;
        }
        LOG.debug("buildPageTime = " + buildPageTime / 1000000 + " ms.");
        //LOG.debug("compressTime = " + compressTime / 1000000 + " ms.");
        //LOG.debug("compressNum = " + compressNum + " .");
        //if (compressNum > 0) {
        //    LOG.debug("Avg per compressTime = " + compressTime / compressNum + " ns.");
        //}
        LOG.debug("completedPositions = " + completedPositions + " .");
        LOG.debug("completedBytes = " + completedBytes + " .");
        LOG.debug("ConnectorPageSource last = " + (System.nanoTime() - startTime) / 1000000 + " ms.");

        closed = true;
        // TODO: close all column reader and value selectors
        try {
            segmentReader.close();
        }
        catch (IOException ioe) {
        }
    }
//    private final class SegmentBlockLoader
//            implements LazyBlockLoader
//    {
//        private final int expectedBatchId = batchId;
//        private final Type type;
//        private final String name;
//        private boolean loaded;
//
//        public SegmentBlockLoader(Type type, String name)
//        {
//            this.type = requireNonNull(type, "type is null");
//            this.name = requireNonNull(name, "name is null");
//        }
//
//        @Override
//        public final Block load()
//        {
//            if (loaded) {
//                return null;
//            }
//
//            checkState(batchId == expectedBatchId);
//
//            Block block = segmentReader.readBlock(type, name);
//            loaded = true;
//            return block;
//        }
//    }
}
