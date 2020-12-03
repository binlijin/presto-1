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

import java.io.IOException;

/**
 * IO engine that stores data to a file on the local block device using memory mapping mechanism
 */
public class ExclusiveMemoryMmapIOEngine
        extends FileMmapIOEngine
{
    public ExclusiveMemoryMmapIOEngine(String filePath, long capacity)
            throws IOException
    {
        super(filePath, capacity);
    }

    @Override
    public Cacheable read(BucketEntry be)
            throws IOException
    {
        ByteBuff dst = be.allocator.allocate(be.getLength());
        bufferArray.read(be.offset(), dst);
        dst.position(0).limit(be.getLength());
        return be.wrapAsCacheable(dst);
    }
}
