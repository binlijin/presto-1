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
package io.prestosql.druid;

import io.prestosql.cache.CacheConfig;
import io.prestosql.cache.CacheFactory;
import io.prestosql.cache.CacheManager;
import io.prestosql.hadoop.FileSystemFactory;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.function.BiFunction;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class DruidCachingHdfsConfiguration
        implements HdfsConfiguration
{
    private final DruidConfig druidConfig;
    private final CacheManager cacheManager;
    private final CacheConfig cacheConfig;
    private final CacheFactory cacheFactory;

    @Inject
    public DruidCachingHdfsConfiguration(
            DruidConfig druidConfig,
            CacheConfig cacheConfig,
            CacheManager cacheManager,
            CacheFactory cacheFactory)
    {
        this.druidConfig = requireNonNull(druidConfig, "druidConfig is null");
        this.cacheManager = requireNonNull(cacheManager, "CacheManager is null");
        this.cacheConfig = requireNonNull(cacheConfig, "cacheConfig is null");
        this.cacheFactory = requireNonNull(cacheFactory, "CacheFactory is null");
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        @SuppressWarnings("resource")
        Configuration config = new CachingJobConf((factoryConfig, factoryUri) -> {
            try {
                FileSystem fileSystem = (new Path(factoryUri)).getFileSystem(druidConfig.readHadoopConfiguration());
                return cacheFactory.createCachingFileSystem(
                        factoryConfig,
                        factoryUri,
                        fileSystem,
                        cacheManager,
                        cacheConfig.isCachingEnabled(),
                        cacheConfig.getCacheType(),
                        cacheConfig.isValidationEnabled());
            }
            catch (IOException e) {
                throw new PrestoException(GENERIC_INTERNAL_ERROR, "cannot create caching file system", e);
            }
        });
        Configuration defaultConfig = druidConfig.readHadoopConfiguration();

        copy(defaultConfig, config);
        return config;
    }

    public static void copy(Configuration from, Configuration to)
    {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }

    private static class CachingJobConf
            extends JobConf
            implements FileSystemFactory
    {
        private final BiFunction<Configuration, URI, FileSystem> factory;

        private CachingJobConf(BiFunction<Configuration, URI, FileSystem> factory)
        {
            super(false);
            this.factory = requireNonNull(factory, "factory is null");
        }

        @Override
        public FileSystem createFileSystem(URI uri)
        {
            return factory.apply(this, uri);
        }
    }
}
