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

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.IndexedIterable;
import org.apache.druid.segment.data.ObjectStrategy;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class ByteBufferGenericIndexed<T>
        implements Indexed<T>, java.io.Closeable
{
    static final byte VERSION_ONE = 0x1;
    static final int NULL_VALUE_SIZE_MARKER = -1;

    private final boolean versionOne;

    private final ObjectStrategy<T> strategy;
    private final boolean allowReverseLookup;
    private final int size;
    private final ByteBuffer headerBuffer;

    private final ByteBuffer firstValueBuffer;

    private final ByteBuffer[] valueBuffers;

    @Nullable
    private final ByteBuffer theBuffer;

    /**
     * Constructor for version one.
     */
    public ByteBufferGenericIndexed(
            ByteBuffer buffer,
            ObjectStrategy<T> strategy,
            boolean allowReverseLookup)
    {
        this.versionOne = true;

        this.theBuffer = buffer;
        this.strategy = strategy;
        this.allowReverseLookup = allowReverseLookup;
        size = theBuffer.getInt();

        int indexOffset = theBuffer.position();
        int valuesOffset = theBuffer.position() + size * Integer.BYTES;

        buffer.position(valuesOffset);
        // Ensure the value buffer's limit equals to capacity.
        firstValueBuffer = buffer.slice();
        valueBuffers = new ByteBuffer[]{firstValueBuffer};
        buffer.position(indexOffset);
        headerBuffer = buffer.slice();
    }

    @Override
    public void close() throws IOException
    {
        // nothing to close
    }

    @Override
    public int size()
    {
        return size;
    }

    @Nullable
    @Override public T get(int index)
    {
        return versionOne ? getVersionOne(index) : null;
    }

    @Nullable
    private T getVersionOne(int index)
    {
        checkIndex(index);

        final int startOffset;
        final int endOffset;

        if (index == 0) {
            startOffset = Integer.BYTES;
            endOffset = headerBuffer.getInt(0);
        }
        else {
            int headerPosition = (index - 1) * Integer.BYTES;
            startOffset = headerBuffer.getInt(headerPosition) + Integer.BYTES;
            endOffset = headerBuffer.getInt(headerPosition + Integer.BYTES);
        }
        return copyBufferAndGet(firstValueBuffer, startOffset, endOffset);
    }

    private void checkIndex(int index)
    {
        if (index < 0) {
            throw new IAE("Index[%s] < 0", index);
        }
        if (index >= size) {
            throw new IAE("Index[%d] >= size[%d]", index, size);
        }
    }

    @Nullable
    private T copyBufferAndGet(ByteBuffer valueBuffer, int startOffset, int endOffset)
    {
        ByteBuffer copyValueBuffer = valueBuffer.asReadOnlyBuffer();
        int size = endOffset - startOffset;
        // When size is 0 and SQL compatibility is enabled also check for null marker before returning null.
        // When SQL compatibility is not enabled return null for both null as well as empty string case.
        if (size == 0 && (NullHandling.replaceWithDefault()
                || copyValueBuffer.get(startOffset - Integer.BYTES) == NULL_VALUE_SIZE_MARKER)) {
            return null;
        }
        copyValueBuffer.position(startOffset);
        // fromByteBuffer must not modify the buffer limit
        return strategy.fromByteBuffer(copyValueBuffer, size);
    }

    @Override
    public int indexOf(@Nullable T value)
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

    @Override
    public Iterator<T> iterator()
    {
        return IndexedIterable.create(this).iterator();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector runtimeShapeInspector)
    {
        //TODO
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("ByteBufferGenericIndexed[");
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

        @Override
        public int size()
        {
            return size;
        }

        @Nullable
        byte[] bufferedIndexedGetByteArray(ByteBuffer copyValueBuffer, int startOffset, int endOffset)
        {
            int size = endOffset - startOffset;
            // When size is 0 and SQL compatibility is enabled also check for null marker before returning null.
            // When SQL compatibility is not enabled return null for both null as well as empty string case.
            if (size == 0 && (NullHandling.replaceWithDefault()
                    || copyValueBuffer.get(startOffset - Integer.BYTES) == NULL_VALUE_SIZE_MARKER)) {
                return null;
            }
            lastReadSize = size;

            // ObjectStrategy.fromByteBuffer() is allowed to reset the limit of the buffer. So if the limit is changed,
            // position() call in the next line could throw an exception, if the position is set beyond the new limit. clear()
            // sets the limit to the maximum possible, the capacity. It is safe to reset the limit to capacity, because the
            // value buffer(s) initial limit equals to capacity.
            copyValueBuffer.clear();
            copyValueBuffer.position(startOffset);
            byte[] bytes = new byte[size];
            copyValueBuffer.get(bytes);
            return bytes;
        }

        @Nullable
        T bufferedIndexedGet(ByteBuffer copyValueBuffer, int startOffset, int endOffset)
        {
            int size = endOffset - startOffset;
            // When size is 0 and SQL compatibility is enabled also check for null marker before returning null.
            // When SQL compatibility is not enabled return null for both null as well as empty string case.
            if (size == 0 && (NullHandling.replaceWithDefault()
                    || copyValueBuffer.get(startOffset - Integer.BYTES) == NULL_VALUE_SIZE_MARKER)) {
                return null;
            }
            lastReadSize = size;

            // ObjectStrategy.fromByteBuffer() is allowed to reset the limit of the buffer. So if the limit is changed,
            // position() call in the next line could throw an exception, if the position is set beyond the new limit. clear()
            // sets the limit to the maximum possible, the capacity. It is safe to reset the limit to capacity, because the
            // value buffer(s) initial limit equals to capacity.
            copyValueBuffer.clear();
            copyValueBuffer.position(startOffset);
            return strategy.fromByteBuffer(copyValueBuffer, size);
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

        @Override
        public int indexOf(@Nullable T value)
        {
            return ByteBufferGenericIndexed.this.indexOf(this, value);
        }

        @Override
        public Iterator<T> iterator()
        {
            return ByteBufferGenericIndexed.this.iterator();
        }

        abstract byte[] getObjectByte(int index);
    }

    public ByteBufferGenericIndexed<T>.BufferIndexed singleThreaded()
    {
        return versionOne ? singleThreadedVersionOne() : null;
    }

    private BufferIndexed singleThreadedVersionOne()
    {
        final ByteBuffer copyBuffer = firstValueBuffer.asReadOnlyBuffer();
        return new BufferIndexed()
        {
            @Override
            public T get(final int index)
            {
                checkIndex(index);

                final int startOffset;
                final int endOffset;

                if (index == 0) {
                    startOffset = Integer.BYTES;
                    endOffset = headerBuffer.getInt(0);
                }
                else {
                    int headerPosition = (index - 1) * Integer.BYTES;
                    startOffset = headerBuffer.getInt(headerPosition) + Integer.BYTES;
                    endOffset = headerBuffer.getInt(headerPosition + Integer.BYTES);
                }
                return bufferedIndexedGet(copyBuffer, startOffset, endOffset);
            }

            @Override
            public void inspectRuntimeShape(RuntimeShapeInspector inspector)
            {
                inspector.visit("headerBuffer", headerBuffer);
                inspector.visit("copyBuffer", copyBuffer);
                inspector.visit("strategy", strategy);
            }

            @Override
            public byte[] getObjectByte(int index)
            {
                checkIndex(index);

                final int startOffset;
                final int endOffset;

                if (index == 0) {
                    startOffset = Integer.BYTES;
                    endOffset = headerBuffer.getInt(0);
                }
                else {
                    int headerPosition = (index - 1) * Integer.BYTES;
                    startOffset = headerBuffer.getInt(headerPosition) + Integer.BYTES;
                    endOffset = headerBuffer.getInt(headerPosition + Integer.BYTES);
                }
                return bufferedIndexedGetByteArray(copyBuffer, startOffset, endOffset);
            }
        };
    }
}
