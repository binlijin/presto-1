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

import io.airlift.slice.Slice;

import static io.prestosql.cache.CacheResult.MISS;

public class NoOpCacheManager
        implements CacheManager
{
    @Override
    public CacheResult get(FileReadRequest request, byte[] buffer, int offset, CacheQuota cacheQuota)
    {
        return MISS;
    }

    @Override
    public void put(FileReadRequest request, Slice data, CacheQuota cacheQuota)
    {
        // no op
    }
}
