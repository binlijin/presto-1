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
package io.prestosql.cache.block.bucket;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FileBlock
        implements Cacheable
{
    public static final long FIXED_OVERHEAD = ClassSize.estimateBase(FileBlock.class, false);

    public static final CacheableDeserializer<Cacheable> BLOCK_DESERIALIZER = new BlockDeserializer();

    public static final class BlockDeserializer
            implements CacheableDeserializer<Cacheable>
    {
        private BlockDeserializer()
        {
        }

        @Override
        public FileBlock deserialize(ByteBuff buf, ByteBuffAllocator alloc)
                throws IOException
        {
            ByteBuff newByteBuff = buf.slice();
            return new FileBlock(newByteBuff, BlockType.DATA, null, UNSET);
        }

        @Override
        public int getDeserializerIdentifier()
        {
            return DESERIALIZER_IDENTIFIER;
        }
    }

    private static final int DESERIALIZER_IDENTIFIER;

    static {
        DESERIALIZER_IDENTIFIER =
                CacheableDeserializerIdManager.registerDeserializer(BLOCK_DESERIALIZER);
    }

    private static final int UNSET = -1;

    // How to get the estimate correctly? if it is a singleBB?
    public static final int MULTI_BYTE_BUFFER_HEAP_SIZE =
            (int) ClassSize.estimateBase(MultiByteBuff.class, false);

    /**
     * Type of block. Header field 0.
     */
    private BlockType blockType;

    /**
     * The in-memory representation of the hfile block. Can be on or offheap. Can be backed by
     * a single ByteBuffer or by many. Make no assumptions.
     *
     * <p>Be careful reading from this <code>buf</code>. Duplicate and work on the duplicate or if
     * not, be sure to reset position and limit else trouble down the road.
     *
     * <p>TODO: Make this read-only once made.
     *
     * <p>We are using the ByteBuff type. ByteBuffer is not extensible yet we need to be able to have
     * a ByteBuffer-like API across multiple ByteBuffers reading from a cache such as BucketCache.
     * So, we have this ByteBuff type. Unfortunately, it is spread all about HFileBlock. Would be
     * good if could be confined to cache-use only but hard-to-do.
     */
    private ByteBuff buf;

    /**
     * The offset of this block in the file. Populated by the reader for
     * convenience of access. This offset is not part of the block header.
     */
    private long offset = UNSET;

    private String fileName;

    public FileBlock(ByteBuff buf, BlockType blockType, String fileName, long offset)
    {
        this.buf = buf;
        this.blockType = blockType;
        this.fileName = fileName;
        this.offset = offset;
    }

    @Override
    public int getSerializedLength()
    {
        if (buf != null) {
            // TODO Include extra bytes for block metadata.
            return this.buf.limit();
        }
        return 0;
    }

    @Override
    public void serialize(ByteBuffer destination, boolean includeNextBlockMetadata)
    {
        this.buf.get(destination, 0, getSerializedLength());

        // Make it ready for reading. flip sets position to zero and limit to current position which
        // is what we want if we do not want to serialize the block plus checksums if present plus
        // metadata.
        destination.flip();
    }

    @Override
    public CacheableDeserializer<Cacheable> getDeserializer()
    {
        return FileBlock.BLOCK_DESERIALIZER;
    }

    @Override
    public BlockType getBlockType()
    {
        return blockType;
    }

    @Override
    public long heapSize()
    {
        long size = FIXED_OVERHEAD;
        if (buf != null) {
            // Deep overhead of the byte buffer. Needs to be aligned separately.
            size += ClassSize.align(buf.capacity() + MULTI_BYTE_BUFFER_HEAP_SIZE);
        }
        return ClassSize.align(size);
    }

    @Override
    public int refCnt()
    {
        return buf.refCnt();
    }

    @Override
    public FileBlock retain()
    {
        buf.retain();
        return this;
    }

    /**
     * Call {@link ByteBuff#release()} to decrease the reference count, if no other reference, it will
     * return back the {@link ByteBuffer} to {link org.apache.hadoop.hbase.io.ByteBuffAllocator}
     */
    @Override
    public boolean release()
    {
        return buf.release();
    }

    /**
     * Returns a read-only duplicate of the buffer this block stores internally ready to be read.
     * Clients must not modify the buffer object though they may set position and limit on the
     * returned buffer since we pass back a duplicate. This method has to be public because it is used
     * in {link CompoundBloomFilter} to avoid object creation on every Bloom
     * filter lookup, but has to be used with caution. Buffer holds header, block content,
     * and any follow-on checksums if present.
     *
     * @return the buffer of this block for read-only operations
     */
    public ByteBuff getBufferReadOnly()
    {
        // TODO: ByteBuf does not support asReadOnlyBuffer(). Fix.
        ByteBuff dup = this.buf.duplicate();
        //assert dup.position() == 0;
        return dup;
    }

    public void get(byte[] dst, int offset, int length)
    {
        this.buf.get(dst, offset, length);
    }

    public byte[] getData()
    {
        byte[] data = new byte[buf.limit()];
        buf.slice().get(data);
        return data;
    }
}
