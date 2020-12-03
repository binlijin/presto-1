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

public class BlockCacheKey
        implements HeapSize, java.io.Serializable
{
    private static final long serialVersionUID = -5199992013113130534L;
    private final String fileName;
    private final long offset;
    private final BlockType blockType;

    /**
     * Construct a new BlockCacheKey
     *
     * @param fileName The name of the HFile this block belongs to.
     * @param offset Offset of the block into the file
     */
    public BlockCacheKey(String fileName, long offset)
    {
        this(fileName, offset, BlockType.DATA);
    }

    public BlockCacheKey(String fileName, long offset, BlockType blockType)
    {
        this.fileName = fileName;
        this.offset = offset;
        this.blockType = blockType;
    }

    @Override
    public int hashCode()
    {
        return fileName.hashCode() * 127 + (int) (offset ^ (offset >>> 32));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o instanceof BlockCacheKey) {
            BlockCacheKey k = (BlockCacheKey) o;
            return offset == k.offset
                    && (fileName == null ? k.fileName == null : fileName
                    .equals(k.fileName));
        }
        else {
            return false;
        }
    }

    @Override
    public String toString()
    {
        return this.fileName + '_' + this.offset;
    }

    public static final long FIXED_OVERHEAD = ClassSize.estimateBase(BlockCacheKey.class, false);

    /**
     * Strings have two bytes per character due to default Java Unicode encoding
     * (hence length times 2).
     */
    @Override
    public long heapSize()
    {
        return ClassSize.align(FIXED_OVERHEAD + ClassSize.STRING +
                2 * fileName.length());
    }

    // can't avoid this unfortunately

    /**
     * @return The fileName portion of this cache key
     */
    public String getFileName()
    {
        return fileName;
    }

    public long getOffset()
    {
        return offset;
    }

    public BlockType getBlockType()
    {
        return blockType;
    }
}
