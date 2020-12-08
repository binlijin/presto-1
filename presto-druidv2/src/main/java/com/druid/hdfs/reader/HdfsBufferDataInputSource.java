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
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import static io.prestosql.druid.DruidErrorCode.DRUID_DEEP_STORAGE_ERROR;
import static java.lang.String.format;

public class HdfsBufferDataInputSource
        implements DataInputSource
{
    private final Path path;
    private final FSDataInputStream inputStream;
    private final long size;
    private AtomicLong readTimeNanos;

    byte[] dataBuffer;
    long startPosition;

    public HdfsBufferDataInputSource(Path path, FSDataInputStream inputStream, long size,
            AtomicLong readTimeNanos)
    {
        this.path = path;
        this.inputStream = inputStream;
        this.size = size;
        this.readTimeNanos = readTimeNanos;

        this.dataBuffer = new byte[Constants.BUFFER_SIZE];
        Arrays.fill(dataBuffer, (byte) 0);
        this.startPosition = -1L;
    }

    @Override
    public Path getPath()
    {
        return path;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos.get();
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
        if (startPosition > 0 && ((position >= startPosition) && (position + bufferLength) <= (
                startPosition + this.dataBuffer.length))) {
            // cache data in dataBuffer
            int srcPos = (int) (position - startPosition);
            System.arraycopy(dataBuffer, srcPos, buffer, bufferOffset, bufferLength);
//            System.out.println("read " + bufferLength + " bytes from cache data buffer.");
//            new IOException("read " + bufferLength + " bytes from cache data buffer.").printStackTrace();
            readTimeNanos.addAndGet(System.nanoTime() - start);
            return;
        }
        if (bufferLength >= Constants.BUFFER_SIZE) {
            readInternal(position, buffer, bufferOffset, bufferLength);
            startPosition = -1L;
            //System.out.println("Direct read " + bufferLength + " bytes from hdfs.");
        }
        else {
            //Arrays.fill(dataBuffer, (byte) 0);
            this.startPosition = position;
            int length = (int) Math.min((size - position), Constants.BUFFER_SIZE);
            if (length < Constants.BUFFER_SIZE) {
                Arrays.fill(dataBuffer, (byte) 0);
            }
            readInternal(position, dataBuffer, 0, length);
            System.arraycopy(dataBuffer, 0, buffer, bufferOffset, bufferLength);
//            System.out.println(
//                    "Read " + Constants.BUFFER_SIZE + " bytes from hdfs and cache, and copy "
//                            + bufferLength + " to upper.");
        }
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
    public boolean equals(Object obj)
    {
        if (obj == this) {
            return true;
        }

        if (obj instanceof HdfsBufferDataInputSource) {
            HdfsBufferDataInputSource other = (HdfsBufferDataInputSource) obj;
            return other.path.equals(this.path);
        }
        else {
            return false;
        }
    }

    @Override
    public int hashCode()
    {
        return path.hashCode();
    }
}
