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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Utilty for aggregating counts in CachedBlocks and toString/toJSON CachedBlocks and BlockCaches.
 * No attempt has been made at making this thread safe.
 */
public class BlockCacheUtil
{
    private static final Logger LOG = LoggerFactory.getLogger(BlockCacheUtil.class);

    public static final long NANOS_PER_SECOND = 1000000000;

    /**
     * @param cb
     * @return The block content as String.
     */
    public static String toString(final CachedBlock cb, final long now)
    {
        return "filename=" + cb.getFilename() + ", " + toStringMinusFileName(cb, now);
    }

    /**
     * Little data structure to hold counts for a file.
     * Used doing a toJSON.
     */
    static class CachedBlockCountsPerFile
    {
        private int count;
        private long size;
        private int countData;
        private long sizeData;
        private final String filename;

        CachedBlockCountsPerFile(final String filename)
        {
            this.filename = filename;
        }

        public int getCount()
        {
            return count;
        }

        public long getSize()
        {
            return size;
        }

        public int getCountData()
        {
            return countData;
        }

        public long getSizeData()
        {
            return sizeData;
        }

        public String getFilename()
        {
            return filename;
        }
    }

    /**
     * @param cb
     * @return The block content of <code>bc</code> as a String minus the filename.
     */
    public static String toStringMinusFileName(final CachedBlock cb, final long now)
    {
        return "offset=" + cb.getOffset() +
                ", size=" + cb.getSize() +
                ", age=" + (now - cb.getCachedTime()) +
                ", type=" + cb.getBlockType() +
                ", priority=" + cb.getBlockPriority();
    }

    private static int compareCacheBlock(Cacheable left, Cacheable right,
            boolean includeNextBlockMetadata)
    {
        ByteBuffer l = ByteBuffer.allocate(left.getSerializedLength());
        left.serialize(l, includeNextBlockMetadata);
        ByteBuffer r = ByteBuffer.allocate(right.getSerializedLength());
        right.serialize(r, includeNextBlockMetadata);
        return Bytes.compareTo(l.array(), l.arrayOffset(), l.limit(),
                r.array(), r.arrayOffset(), r.limit());
    }

    private BlockCacheUtil()
    {
    }
}
