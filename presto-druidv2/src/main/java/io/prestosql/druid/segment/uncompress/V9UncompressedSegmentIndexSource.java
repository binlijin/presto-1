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

import com.druid.hdfs.reader.HDFSIndexIO;
import io.prestosql.druid.DruidColumnHandle;
import io.prestosql.druid.segment.SegmentIndexSource;
import io.prestosql.spi.connector.ColumnHandle;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.QueryableIndex;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class V9UncompressedSegmentIndexSource
        implements SegmentIndexSource
{
    private FileSystem fileSystem;
    private Path segmentPath;

    public V9UncompressedSegmentIndexSource(FileSystem fileSystem, Path segmentPath)
    {
        this.fileSystem = fileSystem;
        this.segmentPath = segmentPath;
    }

    @Override
    public QueryableIndex loadIndex(List<ColumnHandle> columnHandles) throws IOException
    {
        HDFSIndexIO hdfsIndexIO = new HDFSIndexIO(new DefaultObjectMapper(), () -> 0);
        List<String> columns = new ArrayList<>(columnHandles.size());
        for (ColumnHandle columnHandle : columnHandles) {
            String columnName = ((DruidColumnHandle) columnHandle).getColumnName();
            columns.add(columnName);
        }
        return hdfsIndexIO.loadIndex(fileSystem, segmentPath, columns);
    }
}
