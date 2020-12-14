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
package io.prestosql.cache.block;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.prestosql.cache.CacheConfig;
import io.prestosql.cache.CacheManager;
import io.prestosql.cache.CacheQuota;
import io.prestosql.cache.CacheResult;
import io.prestosql.cache.FileReadRequest;
import io.prestosql.cache.block.bucket.BlockCacheKey;
import io.prestosql.cache.block.bucket.BucketCache;
import io.prestosql.cache.block.bucket.Cacheable;
import io.prestosql.cache.block.bucket.FileBlock;
import io.prestosql.cache.block.bucket.SingleByteBuff;

import javax.inject.Inject;

import java.io.IOException;
import java.nio.ByteBuffer;
//import java.util.concurrent.ConcurrentHashMap;

public class BlockCacheCacheManager
        implements CacheManager
{
    private static final Logger log = Logger.get(BlockCacheCacheManager.class);

    //public static ConcurrentHashMap<BlockCacheKey, Cacheable> backingMap = new ConcurrentHashMap<>(1000);

    private BucketCache bucketCache;

    @Inject
    public BlockCacheCacheManager(CacheConfig cacheConfig, BlockCacheConfig blockCacheConfig)
    {
        String bucketCacheIOEngineName = blockCacheConfig.getBucketCacheIOEngineName();
        if (bucketCacheIOEngineName == null || bucketCacheIOEngineName.length() <= 0) {
            bucketCache = null;
        }
        else {
            int blockSize = blockCacheConfig.getBlockSize();
            long bucketCacheSize = blockCacheConfig.getBucketCacheSize();
            int writerThreads = blockCacheConfig.getWriterThreads();
            int writerQueueLen = blockCacheConfig.getWriterQueueLen();
            String persistentPath = blockCacheConfig.getPersistentPath();
            int ioErrorsTolerationDuration = blockCacheConfig.getIoErrorsTolerationDuration();
            try {
                // Bucket cache logs its stats on creation internal to the constructor.
                bucketCache = new BucketCache(
                        bucketCacheIOEngineName,
                        bucketCacheSize,
                        blockSize,
                        null,
                        writerThreads,
                        writerQueueLen,
                        persistentPath,
                        ioErrorsTolerationDuration);
            }
            catch (IOException ioex) {
                log.error("Can't instantiate bucket cache", ioex);
                throw new RuntimeException(ioex);
            }
        }
    }

    @Override
    public CacheResult get(FileReadRequest request, byte[] buffer, int offset,
            CacheQuota cacheQuota)
    {
        if (request.getLength() <= 0) {
            // no-op
            return CacheResult.HIT;
        }
        if (bucketCache != null) {
            BlockCacheKey cacheKey = new BlockCacheKey(request.getFileName(), request.getOffset());
            FileBlock cachedBlock = (FileBlock) bucketCache.getBlock(cacheKey, true, false, true);
            if (cachedBlock != null) {
                //System.out.println("get from cache cacheKey = " + cacheKey + ", dataLen = " + cachedBlock.getSerializedLength());
                if (cachedBlock.getSerializedLength() >= request.getLength()) {
                    cachedBlock.getBufferReadOnly().get(buffer, offset, request.getLength());
                    // TODO
                    cachedBlock.release();
                    return CacheResult.HIT;
                }
            }
        }
        return CacheResult.MISS;
    }

    @Override
    public void put(FileReadRequest request, Slice data, CacheQuota cacheQuota)
    {
        if (bucketCache != null) {
            BlockCacheKey cacheKey = new BlockCacheKey(request.getFileName(), request.getOffset());
            byte[] copy = data.getBytes();
            SingleByteBuff byteBuff = new SingleByteBuff(ByteBuffer.wrap(copy));
            Cacheable buf = new FileBlock(byteBuff, cacheKey.getBlockType(), cacheKey.getFileName(),
                    cacheKey.getOffset());
            bucketCache.cacheBlock(cacheKey, buf);
            //backingMap.put(cacheKey, buf);
        }
    }

    @Override
    public final void shutdown()
    {
        try {
            bucketCache.shutdown();
        }
        catch (Exception e) {
            log.error(e, "Error shutting down connector");
        }
    }
}
