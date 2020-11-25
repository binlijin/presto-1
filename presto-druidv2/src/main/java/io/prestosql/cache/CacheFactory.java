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
package io.prestosql.cache;

import io.prestosql.cache.alluxio.AlluxioCacheConfig;
import io.prestosql.cache.alluxio.AlluxioCachingFileSystem;
import io.prestosql.cache.filemerge.FileMergeCachingFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import javax.inject.Inject;

import java.net.URI;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.cache.CacheType.ALLUXIO;

public class CacheFactory
{
    private final CacheConfig cacheConfig;
    private final AlluxioCacheConfig alluxioCacheConfig;

    @Inject
    public CacheFactory(CacheConfig cacheConfig, AlluxioCacheConfig alluxioCacheConfig)
    {
        this.cacheConfig = cacheConfig;
        this.alluxioCacheConfig = alluxioCacheConfig;
    }

    public FileSystem createCachingFileSystem(
            Configuration factoryConfig,
            URI factoryUri,
            FileSystem fileSystem,
            CacheManager cacheManager,
            boolean cachingEnabled,
            CacheType cacheType,
            boolean validationEnabled)
            throws Exception
    {
        if (!cachingEnabled) {
            return fileSystem;
        }

        checkState(cacheType != null);

        switch (cacheType) {
            case FILE_MERGE:
                return new FileMergeCachingFileSystem(
                        factoryUri,
                        factoryConfig,
                        cacheManager,
                        fileSystem,
                        validationEnabled);
            case ALLUXIO:
                AlluxioCachingFileSystem cachingFileSystem =
                        new AlluxioCachingFileSystem(fileSystem, factoryUri, validationEnabled);
                updateConfiguration(factoryConfig);
                URI uri = new URI("alluxio://test:8020/");
                cachingFileSystem.initialize(uri, factoryConfig);
                return cachingFileSystem;
            default:
                throw new IllegalArgumentException("Invalid CacheType: " + cacheType.name());
        }
    }

    public void updateConfiguration(Configuration configuration)
    {
        if (cacheConfig.isCachingEnabled() && cacheConfig.getCacheType() == ALLUXIO) {
            configuration.set("alluxio.user.local.cache.enabled", String.valueOf(cacheConfig.isCachingEnabled()));
            configuration.set("alluxio.user.client.cache.dir", cacheConfig.getBaseDirectory().getPath());
            configuration.set("alluxio.user.client.cache.size", alluxioCacheConfig.getMaxCacheSize().toString());
            configuration.set("alluxio.user.client.cache.page.size", alluxioCacheConfig.getPageCacheSize().toString());
            configuration.set("alluxio.user.client.cache.async.write.enabled", String.valueOf(alluxioCacheConfig.isAsyncWriteEnabled()));
            configuration.set("alluxio.user.metrics.collection.enabled", String.valueOf(alluxioCacheConfig.isMetricsCollectionEnabled()));
            configuration.set("sink.jmx.class", alluxioCacheConfig.getJmxClass());
            configuration.set("sink.jmx.domain", alluxioCacheConfig.getMetricsDomain());
            configuration.set("alluxio.conf.validation.enabled", String.valueOf(alluxioCacheConfig.isConfigValidationEnabled()));
        }
    }
}
