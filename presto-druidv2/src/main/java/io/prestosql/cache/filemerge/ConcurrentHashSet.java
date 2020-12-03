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

package io.prestosql.cache.filemerge;

import javax.annotation.concurrent.ThreadSafe;

import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A concurrent hash set. This is backed by a {@link ConcurrentHashMap}, and {@link Set}
 * operations are translated to {@link ConcurrentHashMap} operations.
 *
 * @param <T> the type of the set objects
 */
@ThreadSafe
public final class ConcurrentHashSet<T>
        extends AbstractSet<T>
{
    private final ConcurrentHashMap<T, Boolean> mMap;

    /**
     * Creates a new {@link alluxio.collections.ConcurrentHashSet}.
     */
    public ConcurrentHashSet()
    {
        this(2, 0.95f, 1);
    }

    /**
     * Creates a new {@link alluxio.collections.ConcurrentHashSet}.
     *
     * @param initialCapacity the initial capacity
     * @param loadFactor the load factor threshold, used to control resizing
     * @param concurrencyLevel the estimated number of concurrently updating threads
     */
    public ConcurrentHashSet(int initialCapacity, float loadFactor, int concurrencyLevel)
    {
        mMap = new ConcurrentHashMap<>(initialCapacity, loadFactor, concurrencyLevel);
    }

    @Override
    public Iterator<T> iterator()
    {
        return mMap.keySet().iterator();
    }

    @Override
    public int size()
    {
        return mMap.size();
    }

    @Override
    public boolean add(T element)
    {
        return mMap.put(element, Boolean.TRUE) == null;
    }

    /**
     * Adds an element into the set, if and only if it is not already a part of the set.
     *
     * @param element the element to add into the set
     * @return true if this set did not already contain the specified element
     */
    public boolean addIfAbsent(T element)
    {
        return mMap.putIfAbsent(element, Boolean.TRUE) == null;
    }

    @Override
    public void clear()
    {
        mMap.clear();
    }

    @Override
    public boolean contains(Object o)
    {
        return mMap.containsKey(o);
    }

    @Override
    public boolean containsAll(Collection<?> c)
    {
        return mMap.keySet().containsAll(c);
    }

    @Override
    public boolean isEmpty()
    {
        return mMap.isEmpty();
    }

    @Override
    public boolean remove(Object o)
    {
        return mMap.remove(o) != null;
    }

    @Override
    public boolean removeAll(Collection<?> c)
    {
        return mMap.keySet().removeAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c)
    {
        return mMap.keySet().retainAll(c);
    }

    @Override
    public Object[] toArray()
    {
        return mMap.keySet().toArray();
    }

    @Override
    public <E> E[] toArray(E[] a)
    {
        return mMap.keySet().toArray(a);
    }

    @Override
    public String toString()
    {
        return mMap.keySet().toString();
    }
}
