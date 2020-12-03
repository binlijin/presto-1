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

import io.prestosql.cache.block.BlockCache;

import java.util.Iterator;

/**
 * Iterator over an array of BlockCache CachedBlocks.
 */
class BlockCachesIterator
        implements Iterator<CachedBlock>
{
    int index;
    final BlockCache[] bcs;
    Iterator<CachedBlock> current;

    BlockCachesIterator(final BlockCache[] blockCaches)
    {
        this.bcs = blockCaches;
        this.current = this.bcs[this.index].iterator();
    }

    @Override
    public boolean hasNext()
    {
        if (current.hasNext()) {
            return true;
        }
        this.index++;
        if (this.index >= this.bcs.length) {
            return false;
        }
        this.current = this.bcs[this.index].iterator();
        return hasNext();
    }

    @Override
    public CachedBlock next()
    {
        return this.current.next();
    }

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
