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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
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
public class HDFSByteBuffGenericIndexed<T>
        implements Indexed<T>, java.io.Closeable
{
    private static final Logger log = new Logger(HDFSByteBuffGenericIndexed.class);
    static final byte VERSION_ONE = 0x1;
    static final byte VERSION_TWO = 0x2;
    static final byte REVERSE_LOOKUP_ALLOWED = 0x1;
    static final byte REVERSE_LOOKUP_DISALLOWED = 0x0;

    static final int NULL_VALUE_SIZE_MARKER = -1;

    public static <T> HDFSByteBuffGenericIndexed<T> read(HDFSByteBuff buffer, ObjectStrategy<T> strategy)
            throws IOException
    {
        byte versionFromBuffer = buffer.get();

        if (VERSION_ONE == versionFromBuffer) {
            return createGenericIndexedVersionOne(buffer, strategy);
        }
        else if (VERSION_TWO == versionFromBuffer) {
            throw new IAE(
                    "use read(ByteBuffer buffer, ObjectStrategy<T> strategy, SmooshedFileMapper fileMapper)"
                            + " to read version 2 indexed.");
        }
        throw new IAE("Unknown version[%d]", (int) versionFromBuffer);
    }

    public static <T> HDFSByteBuffGenericIndexed<T> read(HDFSByteBuff byteBuff, ObjectStrategy<T> strategy,
            HDFSSmooshedFileMapper fileMapper) throws IOException
    {
        byte versionFromBuffer = byteBuff.get();

        if (VERSION_ONE == versionFromBuffer) {
            return createGenericIndexedVersionOne(byteBuff, strategy);
        }
        else if (VERSION_TWO == versionFromBuffer) {
            //return createGenericIndexedVersionTwo(buffer, strategy, fileMapper);
        }

        throw new IAE("Unknown version [%s]", versionFromBuffer);
    }

    ///////////////
    // VERSION ONE
    ///////////////

    private static <T> HDFSByteBuffGenericIndexed<T> createGenericIndexedVersionOne(HDFSByteBuff byteBuff,
            ObjectStrategy<T> strategy) throws IOException
    {
        boolean allowReverseLookup = byteBuff.get() == REVERSE_LOOKUP_ALLOWED;
        int size = byteBuff.getInt();
        HDFSByteBuff bufferToUse = byteBuff.duplicate();
        bufferToUse.limit(bufferToUse.position() + size);
        byteBuff.position(bufferToUse.limit());
        return new HDFSByteBuffGenericIndexed<>(bufferToUse, strategy, allowReverseLookup);
    }

    private final boolean versionOne;

    private final ObjectStrategy<T> strategy;
    private final boolean allowReverseLookup;
    private final int size;

    private final HDFSByteBuff headerBuffer;
    private ByteBuffer bufferHeader;

    private final HDFSByteBuff firstValueBuffer;
    private final HDFSByteBuff[] valueBuffers;

    private final HDFSByteBuff theBuffer;

    /**
     * Constructor for version one.
     */
    public HDFSByteBuffGenericIndexed(HDFSByteBuff buffer, ObjectStrategy<T> strategy, boolean allowReverseLookup)
            throws IOException
    {
        this.versionOne = true;

        this.theBuffer = buffer;
        this.strategy = strategy;
        this.allowReverseLookup = allowReverseLookup;
        this.size = theBuffer.getInt();

        int indexOffset = theBuffer.position();
        int valuesOffset = theBuffer.position() + size * Integer.BYTES;

        buffer.position(valuesOffset);
        // Ensure the value buffer's limit equals to capacity.
        firstValueBuffer = buffer.slice();
        valueBuffers = new HDFSByteBuff[] {firstValueBuffer};
        buffer.position(indexOffset);
        headerBuffer = buffer.slice();
        bufferHeader = null;
        if (valuesOffset - indexOffset <= 4096) {
            // TODO lazy read headerBuffer
            byte[] data = new byte[valuesOffset - indexOffset];
            headerBuffer.get(data);
            bufferHeader = ByteBuffer.wrap(data);
        }
        else {
            log.debug(" big header buffer " + (valuesOffset - indexOffset));
        }
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

    @Override public void close() throws IOException
    {
    }

    @Override public int size()
    {
        return size;
    }

    @Nullable @Override public T get(int index)
    {
        return versionOne ? getVersionOne(index) : null;
    }

    @Nullable private T getVersionOne(int index)
    {
        try {
            checkIndex(index);

            final int startOffset;
            final int endOffset;

            if (index == 0) {
                startOffset = Integer.BYTES;
                endOffset = getIntFromHeaderBuffer(0);
            }
            else {
                int headerPosition = (index - 1) * Integer.BYTES;
                startOffset = getIntFromHeaderBuffer(headerPosition) + Integer.BYTES;
                endOffset = getIntFromHeaderBuffer(headerPosition + Integer.BYTES);
            }
            return copyBufferAndGet(firstValueBuffer, startOffset, endOffset);
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int getIntFromHeaderBuffer(int index) throws IOException
    {
        if (bufferHeader != null) {
            return bufferHeader.getInt(index);
        }
        else {
            return headerBuffer.getInt(index);
        }
    }

    @Nullable private T copyBufferAndGet(HDFSByteBuff valueBuffer, int startOffset, int endOffset)
            throws IOException
    {
        HDFSByteBuff copyValueBuffer = valueBuffer.duplicate();

        int size = endOffset - startOffset;
        // When size is 0 and SQL compatibility is enabled also check for null marker before returning null.
        // When SQL compatibility is not enabled return null for both null as well as empty string case.
        if (size == 0 && (NullHandling.replaceWithDefault())
                || copyValueBuffer.get(startOffset - Integer.BYTES) == NULL_VALUE_SIZE_MARKER) {
            return null;
        }
        copyValueBuffer.position(startOffset);
        ByteBuffer value = getValue(copyValueBuffer, size);
        // fromByteBuffer must not modify the buffer limit
        return strategy.fromByteBuffer(value, size);
    }

    private ByteBuffer getValue(HDFSByteBuff valueBuffer, int size) throws IOException
    {
        byte[] bytes = new byte[size];
        valueBuffer.get(bytes);
        return ByteBuffer.wrap(bytes);
    }

    @Override public int indexOf(@Nullable T value)
    {
        return indexOf(this, value);
    }

    private int indexOf(Indexed<T> indexed, @Nullable T value)
    {
        if (!allowReverseLookup) {
            throw new UnsupportedOperationException("Reverse lookup not allowed.");
        }

        int minIndex = 0;
        int maxIndex = size - 1;
        while (minIndex <= maxIndex) {
            int currIndex = (minIndex + maxIndex) >>> 1;

            T currValue = indexed.get(currIndex);
            int comparison = strategy.compare(currValue, value);
            if (comparison == 0) {
                return currIndex;
            }

            if (comparison < 0) {
                minIndex = currIndex + 1;
            }
            else {
                maxIndex = currIndex - 1;
            }
        }

        return -(minIndex + 1);
    }

    @Override public Iterator<T> iterator()
    {
        return IndexedIterable.create(this).iterator();
    }

    @Override public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
        inspector.visit("versionOne", versionOne);
        //inspector.visit("headerBuffer", headerBuffer);
        if (versionOne) {
            inspector.visit("firstValueBuffer", firstValueBuffer);
        }
        else {
            // Inspecting just one example of valueBuffer, not needed to inspect the whole array, because all buffers in it
            // are the same.
            inspector.visit("valueBuffer", valueBuffers.length > 0 ? valueBuffers[0] : null);
        }
        inspector.visit("strategy", strategy);
    }

    @Override public String toString()
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
        int lastReadSize;

        @Override public int size()
        {
            return size;
        }

        @Nullable
        byte[] bufferedIndexedGetByteArray(HDFSByteBuff copyValueBuffer, int startOffset, int endOffset)
                throws IOException
        {
            int size = endOffset - startOffset;
            // When size is 0 and SQL compatibility is enabled also check for null marker before returning null.
            // When SQL compatibility is not enabled return null for both null as well as empty string case.
            if (size == 0 && (NullHandling.replaceWithDefault()
                    || copyValueBuffer.get(startOffset - Integer.BYTES)
                    == NULL_VALUE_SIZE_MARKER)) {
                return null;
            }
            lastReadSize = size;

            copyValueBuffer.clear();
            copyValueBuffer.position(startOffset);
            byte[] bytes = new byte[size];
            copyValueBuffer.get(bytes);
            return bytes;
        }

        @Nullable T bufferedIndexedGet(HDFSByteBuff copyValueBuffer, int startOffset, int endOffset)
                throws IOException
        {
            int size = endOffset - startOffset;
            // When size is 0 and SQL compatibility is enabled also check for null marker before returning null.
            // When SQL compatibility is not enabled return null for both null as well as empty string case.
            if (size == 0 && (NullHandling.replaceWithDefault()
                    || copyValueBuffer.get(startOffset - Integer.BYTES)
                    == NULL_VALUE_SIZE_MARKER)) {
                return null;
            }
            lastReadSize = size;

            copyValueBuffer.clear();
            copyValueBuffer.position(startOffset);
            ByteBuffer value = getValue(copyValueBuffer, size);
            return strategy.fromByteBuffer(value, size);
        }

        /**
         * This method makes no guarantees with respect to thread safety
         *
         * @return the size in bytes of the last value read
         */
        int getLastValueSize()
        {
            return lastReadSize;
        }

        @Override public int indexOf(@Nullable T value)
        {
            return HDFSByteBuffGenericIndexed.this.indexOf(this, value);
        }

        @Override public Iterator<T> iterator()
        {
            return HDFSByteBuffGenericIndexed.this.iterator();
        }

        abstract byte[] getObjectByte(int index);
    }

    /**
     * Create a non-thread-safe Indexed, which may perform better than the underlying Indexed.
     *
     * @return a non-thread-safe Indexed
     */
    public HDFSByteBuffGenericIndexed<T>.BufferIndexed singleThreaded()
    {
        return versionOne ? singleThreadedVersionOne() : singleThreadedVersionTwo();
    }

    private BufferIndexed singleThreadedVersionOne()
    {
        final HDFSByteBuff copyBuffer = firstValueBuffer.duplicate();
        return new BufferIndexed() {
            @Override public T get(final int index)
            {
                try {
                    checkIndex(index);

                    final int startOffset;
                    final int endOffset;

                    if (index == 0) {
                        startOffset = Integer.BYTES;
                        endOffset = getIntFromHeaderBuffer(0);
                    }
                    else {
                        int headerPosition = (index - 1) * Integer.BYTES;
                        startOffset = getIntFromHeaderBuffer(headerPosition) + Integer.BYTES;
                        endOffset = getIntFromHeaderBuffer(headerPosition + Integer.BYTES);
                    }
                    return bufferedIndexedGet(copyBuffer, startOffset, endOffset);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
                //inspector.visit("headerBuffer", headerBuffer);
                inspector.visit("copyBuffer", copyBuffer);
                inspector.visit("strategy", strategy);
            }

            @Override
            public byte[] getObjectByte(int index)
            {
                try {
                    checkIndex(index);

                    final int startOffset;
                    final int endOffset;

                    if (index == 0) {
                        startOffset = Integer.BYTES;
                        endOffset = getIntFromHeaderBuffer(0);
                    }
                    else {
                        int headerPosition = (index - 1) * Integer.BYTES;
                        startOffset = getIntFromHeaderBuffer(headerPosition) + Integer.BYTES;
                        endOffset = getIntFromHeaderBuffer(headerPosition + Integer.BYTES);
                    }
                    return bufferedIndexedGetByteArray(copyBuffer, startOffset, endOffset);
                }
                catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }

    private HDFSByteBuffGenericIndexed.BufferIndexed singleThreadedVersionTwo()
    {
        throw new UnsupportedOperationException();
    }
}
