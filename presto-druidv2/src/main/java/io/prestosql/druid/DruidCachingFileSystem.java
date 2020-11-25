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
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.fs.FileSystem;

import javax.inject.Inject;

import java.io.IOException;
import java.net.URI;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class DruidCachingFileSystem
{
    private final DruidConfig druidConfig;
    private final CacheManager cacheManager;
    private final CacheConfig cacheConfig;
    private final CacheFactory cacheFactory;

    @Inject
    public DruidCachingFileSystem(
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

    public FileSystem getCachingFileSystem(FileSystem fileSystem, URI uri)
    {
        try {
            return cacheFactory.createCachingFileSystem(
                    fileSystem.getConf(),
                    uri,
                    fileSystem,
                    cacheManager,
                    cacheConfig.isCachingEnabled(),
                    cacheConfig.getCacheType(),
                    cacheConfig.isValidationEnabled());
        }
        catch (IOException e) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "cannot create caching file system",
                    e);
        }
    }
}
