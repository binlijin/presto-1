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
package com.druid.hdfs.reader.data;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.Indexed;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class HDFSCachingIndexed<T>
        implements Indexed<T>, Closeable
{
    private static final int INITIAL_CACHE_CAPACITY = 16384;

    private static final Logger log = new Logger(HDFSCachingIndexed.class);

    private final HDFSGenericIndexed<T>.BufferIndexed delegate;
    @Nullable private final SizedLRUMap<Integer, T> cachedValues;

    public HDFSCachingIndexed(HDFSGenericIndexed<T> delegate, final int lookupCacheSize)
    {
        this.delegate = delegate.singleThreaded();

        if (lookupCacheSize > 0) {
            log.debug("Allocating column cache of max size[%d]", lookupCacheSize);
            cachedValues = new SizedLRUMap<>(INITIAL_CACHE_CAPACITY, lookupCacheSize);
        }
        else {
            cachedValues = null;
        }
    }

    @Override public int size()
    {
        return delegate.size();
    }

    @Override public T get(int index)
    {
        if (cachedValues != null) {
            final T cached = cachedValues.getValue(index);
            if (cached != null) {
                return cached;
            }

            final T value = delegate.get(index);
            cachedValues.put(index, value, delegate.getLastValueSize());
            return value;
        }
        else {
            return delegate.get(index);
        }
    }

    public byte[] getObjectByte(int index)
    {
        return delegate.getObjectByte(index);
    }

    @Override public int indexOf(@Nullable T value)
    {
        return delegate.indexOf(value);
    }

    @Override public Iterator<T> iterator()
    {
        return delegate.iterator();
    }

    @Override public void close()
    {
        if (cachedValues != null) {
            log.debug("Closing column cache");
            cachedValues.clear();
        }
    }

    @Override public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
        inspector.visit("cachedValues", cachedValues != null);
        inspector.visit("delegate", delegate);
    }

    private static class SizedLRUMap<K, V>
            extends LinkedHashMap<K, Pair<Integer, V>>
    {
        private final int maxBytes;
        private int numBytes;

        SizedLRUMap(int initialCapacity, int maxBytes)
        {
            super(initialCapacity, 0.75f, true);
            this.maxBytes = maxBytes;
            numBytes = 0;
        }

        @Override protected boolean removeEldestEntry(Map.Entry<K, Pair<Integer, V>> eldest)
        {
            if (numBytes > maxBytes) {
                numBytes -= eldest.getValue().lhs;
                return true;
            }
            return false;
        }

        public void put(K key, @Nullable V value, int size)
        {
            final int totalSize = size + 48; // add approximate object overhead
            numBytes += totalSize;
            super.put(key, new Pair<>(totalSize, value));
        }

        @Nullable public V getValue(Object key)
        {
            final Pair<Integer, V> sizeValuePair = super.get(key);
            return sizeValuePair == null ? null : sizeValuePair.rhs;
        }
    }
}
