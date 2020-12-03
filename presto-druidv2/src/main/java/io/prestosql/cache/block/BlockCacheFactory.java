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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BlockCacheFactory
{
    private static final Logger LOG = LoggerFactory.getLogger(BlockCacheFactory.class.getName());

    /**
     * Current ioengine options in include: heap, offheap and file:PATH (where PATH is the path
     * to the file that will host the file-based cache.  See BucketCache#getIOEngineFromName() for
     * list of supported ioengine options.
     * <p>Set this option and a non-zero {@link #BUCKET_CACHE_SIZE_KEY} to enable bucket cache.
     */
    public static final String BUCKET_CACHE_IOENGINE_KEY = "bucketcache.ioengine";

    /**
     * When using bucket cache, this is a float that EITHER represents a percentage of total heap
     * memory size to give to the cache (if &lt; 1.0) OR, it is the capacity in
     * megabytes of the cache.
     */
    public static final String BUCKET_CACHE_SIZE_KEY = "bucketcache.size";

    /**
     * Default block size for an HFile.
     */
    public static final int DEFAULT_BLOCKSIZE = 64 * 1024;

    /**
     * If the chosen ioengine can persist its state across restarts, the path to the file to persist
     * to. This file is NOT the data file. It is a file into which we will serialize the map of
     * what is in the data file. For example, if you pass the following argument as
     * BUCKET_CACHE_IOENGINE_KEY ("hbase.bucketcache.ioengine"),
     * <code>file:/tmp/bucketcache.data </code>, then we will write the bucketcache data to the file
     * <code>/tmp/bucketcache.data</code> but the metadata on where the data is in the supplied file
     * is an in-memory map that needs to be persisted across restarts. Where to store this
     * in-memory state is what you supply here: e.g. <code>/tmp/bucketcache.map</code>.
     */
    public static final String BUCKET_CACHE_PERSISTENT_PATH_KEY = "bucketcache.persistent.path";

    public static final String BUCKET_CACHE_WRITER_THREADS_KEY = "bucketcache.writer.threads";

    public static final String BUCKET_CACHE_WRITER_QUEUE_KEY = "bucketcache.writer.queuelength";

    /**
     * A comma-delimited array of values for use as bucket sizes.
     */
    public static final String BUCKET_CACHE_BUCKETS_KEY = "bucketcache.bucket.sizes";

    /**
     * Defaults for Bucket cache
     */
    public static final int DEFAULT_BUCKET_CACHE_WRITER_THREADS = 3;
    public static final int DEFAULT_BUCKET_CACHE_WRITER_QUEUE = 64;

    /**
     * The target block size used by blockcache instances. Defaults to
     * {link HConstants#DEFAULT_BLOCKSIZE}.
     */
    public static final String BLOCKCACHE_BLOCKSIZE_KEY = "blockcache.minblocksize";

  /*
  public static BlockCache createBlockCache(Configuration conf) {
    FirstLevelBlockCache l1Cache = createFirstLevelCache(conf);
    if (l1Cache == null) {
      return null;
    }
    boolean useExternal = conf.getBoolean(EXTERNAL_BLOCKCACHE_KEY, EXTERNAL_BLOCKCACHE_DEFAULT);
    if (useExternal) {
      BlockCache l2CacheInstance = createExternalBlockcache(conf);
      return l2CacheInstance == null ?
          l1Cache :
          new InclusiveCombinedBlockCache(l1Cache, l2CacheInstance);
    } else {
      // otherwise use the bucket cache.
      BucketCache bucketCache = createBucketCache(conf);
      if (!conf.getBoolean("hbase.bucketcache.combinedcache.enabled", true)) {
        // Non combined mode is off from 2.0
        LOG.warn(
            "From HBase 2.0 onwards only combined mode of LRU cache and bucket cache is available");
      }
      return bucketCache == null ? l1Cache : new CombinedBlockCache(l1Cache, bucketCache);
    }
  }

  private static FirstLevelBlockCache createFirstLevelCache(final Configuration c) {
    final long cacheSize = MemorySizeUtil.getOnHeapCacheSize(c);
    if (cacheSize < 0) {
      return null;
    }
    String policy = c.get(BLOCKCACHE_POLICY_KEY, BLOCKCACHE_POLICY_DEFAULT);
    int blockSize = c.getInt(BLOCKCACHE_BLOCKSIZE_KEY, HConstants.DEFAULT_BLOCKSIZE);
    LOG.info("Allocating BlockCache size=" +
        StringUtils.byteDesc(cacheSize) + ", blockSize=" + StringUtils.byteDesc(blockSize));
    if (policy.equalsIgnoreCase("LRU")) {
      return new LruBlockCache(cacheSize, blockSize, true, c);
    } else {
      throw new IllegalArgumentException("Unknown policy: " + policy);
    }
  }
   */

    /**
     * Enum of all built in external block caches.
     * This is used for config.
     */
  /*
  private static enum ExternalBlockCaches {
    memcached("org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache");
    // TODO(eclark): Consider more. Redis, etc.
    Class<? extends BlockCache> clazz;
    ExternalBlockCaches(String clazzName) {
      try {
        clazz = (Class<? extends BlockCache>) Class.forName(clazzName);
      } catch (ClassNotFoundException cnef) {
        clazz = null;
      }
    }
    ExternalBlockCaches(Class<? extends BlockCache> clazz) {
      this.clazz = clazz;
    }
  }

  private static BlockCache createExternalBlockcache(Configuration c) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Trying to use External l2 cache");
    }
    Class klass = null;

    // Get the class, from the config. s
    try {
      klass = ExternalBlockCaches
          .valueOf(c.get(EXTERNAL_BLOCKCACHE_CLASS_KEY, "memcache")).clazz;
    } catch (IllegalArgumentException exception) {
      try {
        klass = c.getClass(EXTERNAL_BLOCKCACHE_CLASS_KEY, Class.forName(
            "org.apache.hadoop.hbase.io.hfile.MemcachedBlockCache"));
      } catch (ClassNotFoundException e) {
        return null;
      }
    }

    // Now try and create an instance of the block cache.
    try {
      LOG.info("Creating external block cache of type: " + klass);
      return (BlockCache) ReflectionUtils.newInstance(klass, c);
    } catch (Exception e) {
      LOG.warn("Error creating external block cache", e);
    }
    return null;
  }
   */

  /*
  private static BucketCache createBucketCache(Configuration c) {
    // Check for L2.  ioengine name must be non-null.
    String bucketCacheIOEngineName = c.get(BUCKET_CACHE_IOENGINE_KEY, null);
    if (bucketCacheIOEngineName == null || bucketCacheIOEngineName.length() <= 0) {
      return null;
    }

    int blockSize = c.getInt(BLOCKCACHE_BLOCKSIZE_KEY, DEFAULT_BLOCKSIZE);
    final long bucketCacheSize = getBucketCacheSize(c);
    if (bucketCacheSize <= 0) {
      throw new IllegalStateException("bucketCacheSize <= 0; Check " +
          BUCKET_CACHE_SIZE_KEY + " setting and/or server java heap size");
    }
    if (c.get("hbase.bucketcache.percentage.in.combinedcache") != null) {
      LOG.warn("Configuration 'hbase.bucketcache.percentage.in.combinedcache' is no longer "
          + "respected. See comments in http://hbase.apache.org/book.html#_changes_of_note");
    }
    int writerThreads = c.getInt(BUCKET_CACHE_WRITER_THREADS_KEY,
        DEFAULT_BUCKET_CACHE_WRITER_THREADS);
    int writerQueueLen = c.getInt(BUCKET_CACHE_WRITER_QUEUE_KEY,
        DEFAULT_BUCKET_CACHE_WRITER_QUEUE);
    String persistentPath = c.get(BUCKET_CACHE_PERSISTENT_PATH_KEY);
    String[] configuredBucketSizes = c.getStrings(BUCKET_CACHE_BUCKETS_KEY);
    int [] bucketSizes = null;
    if (configuredBucketSizes != null) {
      bucketSizes = new int[configuredBucketSizes.length];
      for (int i = 0; i < configuredBucketSizes.length; i++) {
        int bucketSize = Integer.parseInt(configuredBucketSizes[i].trim());
        if (bucketSize % 256 != 0) {
          // We need all the bucket sizes to be multiples of 256. Having all the configured bucket
          // sizes to be multiples of 256 will ensure that the block offsets within buckets,
          // that are calculated, will also be multiples of 256.
          // See BucketEntry where offset to each block is represented using 5 bytes (instead of 8
          // bytes long). We would like to save heap overhead as less as possible.
          throw new IllegalArgumentException("Illegal value: " + bucketSize + " configured for '"
              + BUCKET_CACHE_BUCKETS_KEY + "'. All bucket sizes to be multiples of 256");
        }
        bucketSizes[i] = bucketSize;
      }
    }
    BucketCache bucketCache = null;
    try {
      int ioErrorsTolerationDuration = c.getInt(
          "hbase.bucketcache.ioengine.errors.tolerated.duration",
          BucketCache.DEFAULT_ERROR_TOLERATION_DURATION);
      // Bucket cache logs its stats on creation internal to the constructor.
      bucketCache = new BucketCache(bucketCacheIOEngineName,
          bucketCacheSize, blockSize, bucketSizes, writerThreads, writerQueueLen, persistentPath,
          ioErrorsTolerationDuration, c);
    } catch (IOException ioex) {
      LOG.error("Can't instantiate bucket cache", ioex); throw new RuntimeException(ioex);
    }
    return bucketCache;
  }
  */

    /**
     * param conf used to read config for bucket cache size. (< 1 is treated as % and > is treated as MiB)
     *
     * @return the number of bytes to use for bucket cache, negative if disabled.
     */
    /*
    public static long getBucketCacheSize(final Configuration conf) {
        // Size configured in MBs
        float bucketCacheSize = conf.getFloat(BUCKET_CACHE_SIZE_KEY, 0F);
        if (bucketCacheSize < 1) {
            throw new IllegalArgumentException("Bucket Cache should be minimum 1 MB in size."
                    + "Configure 'hbase.bucketcache.size' with > 1 value");
        }
        return (long) (bucketCacheSize * 1024 * 1024);
    }
    */
    private BlockCacheFactory(BlockCacheConfig blockCacheConfig)
    {
    }
}
