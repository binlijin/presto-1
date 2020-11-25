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
package io.prestosql.cache.alluxio;

import alluxio.hadoop.LocalCacheFileSystem;
import io.prestosql.cache.CachingFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

public class AlluxioCachingFileSystem
        extends CachingFileSystem
{
    private static final int BUFFER_SIZE = 65536;
    private final boolean cacheValidationEnabled;
    private LocalCacheFileSystem localCacheFileSystem;

    public AlluxioCachingFileSystem(FileSystem dataTier, URI uri)
    {
        this(dataTier, uri, false);
    }

    public AlluxioCachingFileSystem(FileSystem dataTier, URI uri, boolean cacheValidationEnabled)
    {
        super(dataTier, uri);
        this.cacheValidationEnabled = cacheValidationEnabled;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration configuration)
            throws IOException
    {
        this.localCacheFileSystem = new LocalCacheFileSystem(dataTier);
        localCacheFileSystem.initialize(uri, configuration);
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        FSDataInputStream cachingInputStream = localCacheFileSystem.open(path, BUFFER_SIZE);
        if (cacheValidationEnabled) {
            return new CacheValidatingInputStream(cachingInputStream,
                    dataTier.open(path, bufferSize));
        }
        return cachingInputStream;
    }
}
