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
 * Thrown by {@link BucketAllocator#allocateBlock(int)} when cache is full for
 * the requested size
 */
public class CacheFullException
        extends IOException
{
    private static final long serialVersionUID = 3265127301824638920L;
    private final int requestedSize;
    private final int bucketIndex;

    CacheFullException(int requestedSize, int bucketIndex)
    {
        super();
        this.requestedSize = requestedSize;
        this.bucketIndex = bucketIndex;
    }

    public int bucketIndex()
    {
        return bucketIndex;
    }

    public int requestedSize()
    {
        return requestedSize;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder(1024);
        sb.append("Allocator requested size ").append(requestedSize);
        sb.append(" for bucket ").append(bucketIndex);
        return sb.toString();
    }
}
