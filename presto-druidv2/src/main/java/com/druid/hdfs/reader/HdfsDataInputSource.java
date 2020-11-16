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
package com.druid.hdfs.reader;

import io.prestosql.spi.PrestoException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static io.prestosql.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
import static java.lang.String.format;

public class HdfsDataInputSource
        implements DataInputSource
{
    private final Path path;
    private final FSDataInputStream inputStream;
    private AtomicLong readTimeNanos;

    public HdfsDataInputSource(Path path, FSDataInputStream inputStream, AtomicLong readTimeNanos)
    {
        this.path = path;
        this.inputStream = inputStream;
    }

    @Override
    public Path getPath()
    {
        return path;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public void readFully(long position, byte[] buffer) throws IOException
    {
        readFully(position, buffer, 0, buffer.length);
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        long start = System.nanoTime();
        readInternal(position, buffer, bufferOffset, bufferLength);

        readTimeNanos.addAndGet(System.nanoTime() - start);
    }

    private void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
    {
        try {
            inputStream.readFully(position, buffer, bufferOffset, bufferLength);
        }
        catch (IOException e) {
            throw new PrestoException(DRUID_DEEP_STORAGE_ERROR, format("Error reading from %s at position %s", path, position), e);
        }
    }

    @Override
    public String toString()
    {
        return path.toString();
    }

    @Override
    public boolean equals(Object object)
    {
        if (!(object instanceof HdfsDataInputSource)) {
            return false;
        }
        return ((HdfsDataInputSource) object).path.equals(this.path);
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }
}
