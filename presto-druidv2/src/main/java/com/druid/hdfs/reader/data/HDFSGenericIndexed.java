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

import com.druid.hdfs.reader.HDFSSmooshedFileMapper;
import com.druid.hdfs.reader.utils.HDFSByteBuff;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 * A generic, flat storage mechanism.  Use static methods fromArray() or fromIterable() to construct.  If input
 * is sorted, supports binary search index lookups.  If input is not sorted, only supports array-like index lookups.
 * <p>
 * V1 Storage Format:
 * <p>
 * byte 1: version (0x1)
 * byte 2 == 0x1 =>; allowReverseLookup
 * bytes 3-6 =>; numBytesUsed
 * bytes 7-10 =>; numElements
 * bytes 10-((numElements * 4) + 10): integers representing *end* offsets of byte serialized values
 * bytes ((numElements * 4) + 10)-(numBytesUsed + 2): 4-byte integer representing length of value, followed by bytes
 * for value. Length of value stored has no meaning, if next offset is strictly greater than the current offset,
 * and if they are the same, -1 at this field means null, and 0 at this field means some object
 * (potentially non-null - e. g. in the string case, that is serialized as an empty sequence of bytes).
 * <p>
 * V2 Storage Format
 * Meta, header and value files are separate and header file stored in native endian byte order.
 * Meta File:
 * byte 1: version (0x2)
 * byte 2 == 0x1 =>; allowReverseLookup
 * bytes 3-6: numberOfElementsPerValueFile expressed as power of 2. That means all the value files contains same
 * number of items except last value file and may have fewer elements.
 * bytes 7-10 =>; numElements
 * bytes 11-14 =>; columnNameLength
 * bytes 15-columnNameLength =>; columnName
 * <p>
 * Header file name is identified as: StringUtils.format("%s_header", columnName)
 * value files are identified as: StringUtils.format("%s_value_%d", columnName, fileNumber)
 * number of value files == numElements/numberOfElementsPerValueFile
 */
public class HDFSGenericIndexed<T>
        implements Indexed<T>, java.io.Closeable
{
    private static final Logger log = new Logger(HDFSGenericIndexed.class);
    static final byte VERSION_ONE = 0x1;
    static final byte VERSION_TWO = 0x2;
    static final byte REVERSE_LOOKUP_ALLOWED = 0x1;

    public static <T> HDFSGenericIndexed<T> read(HDFSByteBuff buffer, ObjectStrategy<T> strategy)
            throws IOException
    {
        byte versionFromBuffer = buffer.get();

        if (VERSION_ONE == versionFromBuffer) {
            return createGenericIndexedVersionOne(buffer, strategy, false);
        }
        else if (VERSION_TWO == versionFromBuffer) {
            throw new IAE(
                    "use read(ByteBuffer buffer, ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)"
                            + " to read version 2 indexed.");
        }
        throw new IAE("Unknown version[%d]", (int) versionFromBuffer);
    }

    public static <T> HDFSGenericIndexed<T> read(HDFSByteBuff buffer, ObjectStrategy<T> strategy, boolean readahead)
            throws IOException
    {
        byte versionFromBuffer = buffer.get();

        if (VERSION_ONE == versionFromBuffer) {
            return createGenericIndexedVersionOne(buffer, strategy, readahead);
        }
        else if (VERSION_TWO == versionFromBuffer) {
            throw new IAE(
                    "use read(ByteBuffer buffer, ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)"
                            + " to read version 2 indexed.");
        }
        throw new IAE("Unknown version[%d]", (int) versionFromBuffer);
    }

    public static <T> HDFSGenericIndexed<T> read(HDFSByteBuff byteBuff, ObjectStrategy<T> strategy,
            HDFSSmooshedFileMapper fileMapper) throws IOException
    {
        byte versionFromBuffer = byteBuff.get();

        if (VERSION_ONE == versionFromBuffer) {
            return createGenericIndexedVersionOne(byteBuff, strategy, false);
        }
        else if (VERSION_TWO == versionFromBuffer) {
            //return createGenericIndexedVersionTwo(buffer, strategy, fileMapper);
        }

        throw new IAE("Unknown version [%s]", versionFromBuffer);
    }

    ///////////////
    // VERSION ONE
    ///////////////

    private static <T> HDFSGenericIndexed<T> createGenericIndexedVersionOne(HDFSByteBuff byteBuff,
            ObjectStrategy<T> strategy, boolean readahead) throws IOException
    {
        boolean allowReverseLookup = byteBuff.get() == REVERSE_LOOKUP_ALLOWED;
        int size = byteBuff.getInt();
        HDFSByteBuff bufferToUse = byteBuff.duplicate();
        bufferToUse.limit(bufferToUse.position() + size);
        byteBuff.position(bufferToUse.limit());
        if (readahead) {
            if (log.isDebugEnabled()) {
                log.debug("read ahead size " + size);
            }
            byte[] data = new byte[size];
            bufferToUse.get(data);
            ByteBufferGenericIndexed<T> byteBufferGenericIndexed =
                    new ByteBufferGenericIndexed<T>(ByteBuffer.wrap(data), strategy,
                            allowReverseLookup);
            return new HDFSGenericIndexed(byteBufferGenericIndexed);
        }
        else {
            HDFSByteBuffGenericIndexed<T> hdfsByteBuffGenericIndexed =
                    new HDFSByteBuffGenericIndexed<T>(bufferToUse, strategy, allowReverseLookup);
            return new HDFSGenericIndexed(hdfsByteBuffGenericIndexed);
        }
    }

    private final int size;
    private HDFSByteBuffGenericIndexed<T> hdfsByteBuffGenericIndexed;
    private ByteBufferGenericIndexed<T> byteBufferGenericIndexed;

    /**
     * Constructor for version one.
     */
    HDFSGenericIndexed(HDFSByteBuffGenericIndexed<T> hdfsByteBuffGenericIndexed)
    {
        this.size = hdfsByteBuffGenericIndexed.size();
        this.hdfsByteBuffGenericIndexed = hdfsByteBuffGenericIndexed;
    }

    HDFSGenericIndexed(ByteBufferGenericIndexed<T> byteBufferGenericIndexed)
    {
        this.size = byteBufferGenericIndexed.size();
        this.byteBufferGenericIndexed = byteBufferGenericIndexed;
    }

    /**
     * Checks  if {@code index} a valid `element index` in GenericIndexed.
     * Similar to Preconditions.checkElementIndex() except this method throws {@link IAE} with custom error message.
     * <p>
     * Used here to get existing behavior(same error message and exception) of V1 GenericIndexed.
     *
     * @param index index identifying an element of an GenericIndexed.
     */
    private void checkIndex(int index)
    {
        if (index < 0) {
            throw new IAE("Index[%s] < 0", index);
        }
        if (index >= size) {
            throw new IAE("Index[%d] >= size[%d]", index, size);
        }
    }

    @Override
    public void close() throws IOException
    {
    }

    @Override
    public int size()
    {
        return size;
    }

    @Nullable
    @Override
    public T get(int index)
    {
        if (hdfsByteBuffGenericIndexed != null) {
            return hdfsByteBuffGenericIndexed.get(index);
        }
        else {
            return byteBufferGenericIndexed.get(index);
        }
    }

    @Override
    public int indexOf(@Nullable T value)
    {
        if (hdfsByteBuffGenericIndexed != null) {
            return hdfsByteBuffGenericIndexed.indexOf(value);
        }
        else {
            return byteBufferGenericIndexed.indexOf(value);
        }
    }

    @Override
    public Iterator<T> iterator()
    {
        return IndexedIterable.create(this).iterator();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("HDFSGenericIndexedV2[");
        if (size() > 0) {
            for (int i = 0; i < size(); i++) {
                T value = get(i);
                sb.append(value).append(',').append(' ');
            }
            sb.setLength(sb.length() - 2);
        }
        sb.append(']');
        return sb.toString();
    }

    abstract class BufferIndexed
            implements Indexed<T>
    {
        @Override
        public int size()
        {
            return size;
        }

        /**
         * This method makes no guarantees with respect to thread safety
         *
         * @return the size in bytes of the last value read
         */
        abstract int getLastValueSize();

        @Override
        public Iterator<T> iterator()
        {
            return HDFSGenericIndexed.this.iterator();
        }
    }

    /**
     * Create a non-thread-safe Indexed, which may perform better than the underlying Indexed.
     *
     * @return a non-thread-safe Indexed
     */
    public HDFSGenericIndexed<T>.BufferIndexed singleThreaded()
    {
        if (hdfsByteBuffGenericIndexed != null) {
            final HDFSByteBuffGenericIndexed<T>.BufferIndexed bufferIndexed =
                    hdfsByteBuffGenericIndexed.singleThreaded();
            return new BufferIndexed() {
                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector runtimeShapeInspector)
                {
                }

                @Nullable
                @Override
                public T get(int i)
                {
                    return bufferIndexed.get(i);
                }

                @Override
                public int indexOf(@Nullable T value)
                {
                    return bufferIndexed.indexOf(value);
                }

                int getLastValueSize()
                {
                    return bufferIndexed.getLastValueSize();
                }
            };
        }
        else {
            final ByteBufferGenericIndexed<T>.BufferIndexed bufferIndexed =
                    byteBufferGenericIndexed.singleThreaded();
            return new BufferIndexed() {
                @Override
                public void inspectRuntimeShape(RuntimeShapeInspector runtimeShapeInspector)
                {
                }

                @Nullable
                @Override
                public T get(int i)
                {
                    return bufferIndexed.get(i);
                }

                @Override
                public int indexOf(@Nullable T value)
                {
                    return bufferIndexed.indexOf(value);
                }

                int getLastValueSize()
                {
                    return bufferIndexed.getLastValueSize();
                }
            };
        }
    }

    private HDFSGenericIndexed.BufferIndexed singleThreadedVersionTwo()
    {
        throw new UnsupportedOperationException();
    }
}
