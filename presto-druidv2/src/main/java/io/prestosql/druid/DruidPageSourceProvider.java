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
package io.prestosql.druid;

import io.airlift.log.Logger;
import io.prestosql.druid.metadata.DruidSegmentInfo;
import io.prestosql.druid.segment.DruidSegmentReader;
import io.prestosql.druid.segment.HdfsDataInputSource;
import io.prestosql.druid.segment.IndexFileSource;
import io.prestosql.druid.segment.SegmentColumnSource;
import io.prestosql.druid.segment.SegmentIndexSource;
import io.prestosql.druid.segment.SmooshedColumnSource;
import io.prestosql.druid.segment.V9SegmentIndexSource;
import io.prestosql.druid.segment.ZipIndexFileSource;
import io.prestosql.druid.uncompress.DruidUncompressedSegmentReader;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.prestosql.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
import static io.prestosql.druid.DruidSplit.SplitType.BROKER;
import static java.util.Objects.requireNonNull;

public class DruidPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private static final Logger LOG = Logger.get(DruidPageSourceProvider.class);

    private final DruidClient druidClient;
    private final DruidConfig config;
    private final Configuration hadoopConfiguration;
    private final DruidCachingFileSystem druidCachingFileSystem;

    @Inject
    public DruidPageSourceProvider(
            DruidClient druidClient,
            DruidConfig config,
            DruidCachingFileSystem druidCachingFileSystem)
    {
        this.druidClient = requireNonNull(druidClient, "druid client is null");
        this.config = requireNonNull(config, "druid config is null");
        this.hadoopConfiguration = config.readHadoopConfiguration();
        this.druidCachingFileSystem = druidCachingFileSystem;
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        DruidSplit druidSplit = (DruidSplit) split;
        if (druidSplit.getSplitType() == BROKER) {
            return new DruidBrokerPageSource(
                    columns,
                    druidClient);
        }
        DruidTableHandle druidTableHandle = (DruidTableHandle) tableHandle;
        List<DruidColumnHandle> handles = new ArrayList<>();
        for (ColumnHandle handle : columns) {
            handles.add((DruidColumnHandle) handle);
        }
        // convert push down filter to druid filter.
        DimFilter filter = DruidFilterConverter.generateFilter(druidTableHandle, handles);
        long limit = Long.MAX_VALUE;
        if (druidTableHandle.getLimit().isPresent()) {
            limit = druidTableHandle.getLimit().getAsLong();
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("filter = " + filter + "limit = " + limit);
        }

        DruidSegmentInfo segmentInfo = druidSplit.getSegmentInfo().get();
        try {
            Path segmentPath = new Path(segmentInfo.getDeepStoragePath());
            FileSystem fileSystem = druidCachingFileSystem.getCachingFileSystem(
                    segmentPath.getFileSystem(hadoopConfiguration),
                    segmentPath.toUri());
            if (fileSystem.isDirectory(segmentPath)) {
                // TODO
                DruidUncompressedSegmentReader reader = new DruidUncompressedSegmentReader(
                        fileSystem,
                        segmentPath,
                        config.getMaxBatchSize(),
                        columns,
                        filter,
                        limit);
                return new DruidUncompressedSegmentPageSource(columns, reader);
            }
            else {
                long fileSize = fileSystem.getFileStatus(segmentPath).getLen();
                FSDataInputStream inputStream = fileSystem.open(segmentPath);
                DataInputSourceId dataInputSourceId = new DataInputSourceId(segmentPath.toString());
                HdfsDataInputSource dataInputSource =
                        new HdfsDataInputSource(dataInputSourceId, inputStream, fileSize);
                IndexFileSource indexFileSource = new ZipIndexFileSource(dataInputSource);
                SegmentColumnSource segmentColumnSource = new SmooshedColumnSource(indexFileSource);
                SegmentIndexSource segmentIndexSource = new V9SegmentIndexSource(segmentColumnSource);

                return new DruidSegmentPageSource(dataInputSource, columns,
                        new DruidSegmentReader(segmentIndexSource, columns, filter, limit));
            }
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, "Failed to create page source on " + segmentInfo.getDeepStoragePath(), e);
        }
    }
}
