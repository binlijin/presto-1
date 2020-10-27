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
package com.druid.hdfs.reader.data;

import com.druid.hdfs.reader.utils.HDFSByteBuff;
import com.google.common.base.Preconditions;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.smoosh.FileSmoosher;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.ColumnarInts;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.WritableSupplier;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;

public class HDFSCompressedVSizeColumnarIntsSupplier
        implements WritableSupplier<ColumnarInts>
{
    private static final Logger log = new Logger(HDFSCompressedVSizeColumnarIntsSupplier.class);

    public static final byte VERSION = 0x2;

    private final int totalSize;
    private final int sizePer;
    private final int numBytes;
    private final int bigEndianShift;
    private final int littleEndianMask;
    private final HDFSGenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers;
    private final CompressionStrategy compression;

    private HDFSCompressedVSizeColumnarIntsSupplier(
            int totalSize,
            int sizePer,
            int numBytes,
            HDFSGenericIndexed<ResourceHolder<ByteBuffer>> baseBuffers,
            CompressionStrategy compression)
    {
        Preconditions.checkArgument(sizePer == (1 << Integer.numberOfTrailingZeros(sizePer)),
                "Number of entries per chunk must be a power of 2");

        this.totalSize = totalSize;
        this.sizePer = sizePer;
        this.baseBuffers = baseBuffers;
        this.compression = compression;
        this.numBytes = numBytes;
        this.bigEndianShift = Integer.SIZE - (numBytes << 3); // numBytes * 8
        this.littleEndianMask =
                (int) ((1L << (numBytes << 3)) - 1); // set numBytes * 8 lower bits to 1
    }

    @Override public ColumnarInts get()
    {
        // optimized versions for int, short, and byte columns
        if (numBytes == Integer.BYTES) {
            return new CompressedFullSizeColumnarInts();
        }
        else if (numBytes == Short.BYTES) {
            return new CompressedShortSizeColumnarInts();
        }
        else if (numBytes == 1) {
            return new CompressedByteSizeColumnarInts();
        }
        else {
            // default version of everything else, i.e. 3-bytes per value
            return new CompressedVSizeColumnarInts();
        }
    }

    @Override public long getSerializedSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override public void writeTo(WritableByteChannel channel, FileSmoosher smoosher)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    public static HDFSCompressedVSizeColumnarIntsSupplier fromByteBuffer(HDFSByteBuff buffer,
            ByteOrder order) throws IOException
    {
        byte versionFromBuffer = buffer.get();

        if (versionFromBuffer == VERSION) {
            final int numBytes = buffer.get();
            final int totalSize = buffer.getInt();
            final int sizePer = buffer.getInt();

            final CompressionStrategy compression = CompressionStrategy.forId(buffer.get());

            //numBytes = 2, totalSize = 39244, sizePer = 32768, compression = lz4

            return new HDFSCompressedVSizeColumnarIntsSupplier(
                    totalSize,
                    sizePer,
                    numBytes,
                    HDFSGenericIndexed.read(buffer,
                            new HDFSDecompressingByteBufferObjectStrategy(order, compression)),
                    compression);
        }

        throw new IAE("Unknown version[%s]", versionFromBuffer);
    }

    private class CompressedFullSizeColumnarInts
            extends CompressedVSizeColumnarInts
    {
        @Override protected int _get(ByteBuffer buffer, boolean bigEngian, int bufferIndex)
        {
            return buffer.getInt(bufferIndex * Integer.BYTES);
        }
    }

    private class CompressedShortSizeColumnarInts
            extends CompressedVSizeColumnarInts
    {
        @Override protected int _get(ByteBuffer buffer, boolean bigEngian, int bufferIndex)
        {
            // removes the need for padding
            return buffer.getShort(bufferIndex * Short.BYTES) & 0xFFFF;
        }
    }

    private class CompressedByteSizeColumnarInts
            extends CompressedVSizeColumnarInts
    {
        @Override protected int _get(ByteBuffer buffer, boolean bigEngian, int bufferIndex)
        {
            // removes the need for padding
            return buffer.get(bufferIndex) & 0xFF;
        }
    }

    private class CompressedVSizeColumnarInts
            implements ColumnarInts
    {
        final Indexed<ResourceHolder<ByteBuffer>> singleThreadedBuffers =
                baseBuffers.singleThreaded();

        final int div = Integer.numberOfTrailingZeros(sizePer);
        final int rem = sizePer - 1;

        int currBufferNum = -1;
        ResourceHolder<ByteBuffer> holder;
        /**
         * buffer's position must be 0
         */
        ByteBuffer buffer;
        boolean bigEndian;

        @Override public int size()
        {
            return totalSize;
        }

        /**
         * Returns the value at the given index into the column.
         * <p/>
         * Assumes the number of entries in each decompression buffers is a power of two.
         *
         * @param index index of the value in the column
         * @return the value at the given index
         */
        @Override public int get(int index)
        {
            // assumes the number of entries in each buffer is a power of 2
            final int bufferNum = index >> div;
            final int bufferIndex = index & rem;

            if (bufferNum != currBufferNum) {
                loadBuffer(bufferNum);
            }

            return _get(buffer, bigEndian, bufferIndex);
        }

        @Override public void get(int[] out, int start, int length)
        {
            int p = 0;

            while (p < length) {
                // assumes the number of entries in each buffer is a power of 2
                final int bufferNum = (start + p) >> div;
                if (bufferNum != currBufferNum) {
                    loadBuffer(bufferNum);
                }

                final int currBufferStart = bufferNum * sizePer;
                final int nextBufferStart = currBufferStart + sizePer;

                int i;
                for (i = p; i < length; i++) {
                    final int index = start + i;
                    if (index >= nextBufferStart) {
                        break;
                    }

                    out[i] = _get(buffer, bigEndian, index - currBufferStart);
                }

                //assert i > p;
                p = i;
            }
        }

        @Override public void get(final int[] out, final int[] indexes, final int length)
        {
            int p = 0;

            while (p < length) {
                // assumes the number of entries in each buffer is a power of 2
                final int bufferNum = indexes[p] >> div;
                if (bufferNum != currBufferNum) {
                    loadBuffer(bufferNum);
                }

                final int currBufferStart = bufferNum * sizePer;
                final int nextBufferStart = currBufferStart + sizePer;

                int i;
                for (i = p; i < length; i++) {
                    final int index = indexes[i];
                    if (index >= nextBufferStart) {
                        break;
                    }

                    out[i] = _get(buffer, bigEndian, index - currBufferStart);
                }

                //assert i > p;
                p = i;
            }
        }

        /**
         * Returns the value at the given bufferIndex in the current decompression buffer
         *
         * @param bufferIndex index of the value in the current buffer
         * @return the value at the given bufferIndex
         */
        int _get(ByteBuffer buffer, boolean bigEndian, final int bufferIndex)
        {
            final int pos = bufferIndex * numBytes;
            // example for numBytes = 3
            // big-endian:    0x000c0b0a stored 0c 0b 0a XX, read 0x0c0b0aXX >>> 8
            // little-endian: 0x000c0b0a stored 0a 0b 0c XX, read 0xXX0c0b0a & 0x00FFFFFF
            return bigEndian ?
                    buffer.getInt(pos) >>> bigEndianShift :
                    buffer.getInt(pos) & littleEndianMask;
        }

        protected void loadBuffer(int bufferNum)
        {
            CloseQuietly.close(holder);
            holder = singleThreadedBuffers.get(bufferNum);
            ByteBuffer bb = holder.get();
            ByteOrder byteOrder = bb.order();
            // slice() makes the buffer's position = 0
            buffer = bb.slice().order(byteOrder);
            currBufferNum = bufferNum;
            bigEndian = byteOrder.equals(ByteOrder.BIG_ENDIAN);
        }

        @Override public void close()
        {
            if (holder != null) {
                holder.close();
            }
        }

        @Override public String toString()
        {
            return "CompressedVSizeColumnarInts{"
                    + "currBufferNum=" + currBufferNum
                    + ", sizePer=" + sizePer
                    + ", numChunks=" + singleThreadedBuffers.size()
                    + ", totalSize=" + totalSize + '}';
        }

        @Override public void inspectRuntimeShape(RuntimeShapeInspector inspector)
        {
            // ideally should inspect buffer and bigEndian, but at the moment of inspectRuntimeShape() call buffer is likely
            // to be null and bigEndian = false, because loadBuffer() is not yet called, although during the processing buffer
            // is not null, hence "visiting" null is not representative, and visiting bigEndian = false could be misleading.
            inspector.visit("singleThreadedBuffers", singleThreadedBuffers);
        }
    }
}
