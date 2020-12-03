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

import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;

import javax.validation.constraints.Min;

public class BlockCacheConfig
{
    private String bucketCacheIOEngineName;
    private int blockSize = 64 * 1024;
    private long bucketCacheSize = 1 * 1024 * 1024;
    private int writerThreads;
    private int writerQueueLen;
    private String persistentPath;
    private int ioErrorsTolerationDuration = 60 * 1000;

    public String getBucketCacheIOEngineName()
    {
        return bucketCacheIOEngineName;
    }

    @Config("bucketcache.ioengine")
    @ConfigDescription("ioengine options to cache data")
    public BlockCacheConfig setBucketCacheIOEngineName(String bucketCacheIOEngineName)
    {
        this.bucketCacheIOEngineName = bucketCacheIOEngineName;
        return this;
    }

    @Min(1024)
    public int getBlockSize()
    {
        return blockSize;
    }

    @Config("blockcache.minblocksize")
    @ConfigDescription("Default block size for an File.")
    public BlockCacheConfig setBlockSize(int blockSize)
    {
        this.blockSize = blockSize;
        return this;
    }

    public long getBucketCacheSize()
    {
        return bucketCacheSize;
    }

    @Config("bucketcache.size")
    @ConfigDescription("It is the capacity of the cache.")
    public BlockCacheConfig setBucketCacheSize(int bucketCacheSize)
    {
        this.bucketCacheSize = bucketCacheSize;
        return this;
    }

    public int getWriterThreads()
    {
        return writerThreads;
    }

    @Config("bucketcache.writer.threads")
    @ConfigDescription("bucketcache writer threads")
    public BlockCacheConfig setWriterThreads(int writerThreads)
    {
        this.writerThreads = writerThreads;
        return this;
    }

    public int getWriterQueueLen()
    {
        return writerQueueLen;
    }

    @Config("bucketcache.writer.queuelength")
    @ConfigDescription("bucketcache writer queuelength")
    public BlockCacheConfig setWriterQueueLen(int writerQueueLen)
    {
        this.writerQueueLen = writerQueueLen;
        return this;
    }

    public String getPersistentPath()
    {
        return persistentPath;
    }

    @Config("bucketcache.persistent.path")
    @ConfigDescription("It is a file into which we will serialize the map of what is in the data file.")
    public BlockCacheConfig setPersistentPath(String persistentPath)
    {
        this.persistentPath = persistentPath;
        return this;
    }

    public int getIoErrorsTolerationDuration()
    {
        return ioErrorsTolerationDuration;
    }

    @Config("bucketcache.ioengine.errors.tolerated.duration")
    @ConfigDescription("bucketcache ioengine errors tolerated duration")
    public BlockCacheConfig setIoErrorsTolerationDuration(int ioErrorsTolerationDuration)
    {
        this.ioErrorsTolerationDuration = ioErrorsTolerationDuration;
        return this;
    }
}
