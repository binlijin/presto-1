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

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Allows multiple concurrent clients to lock on a numeric id with ReentrantReadWriteLock. The
 * intended usage for read lock is as follows:
 *
 * <pre>
 * ReentrantReadWriteLock lock = idReadWriteLock.getLock(id);
 * try {
 *   lock.readLock().lock();
 *   // User code.
 * } finally {
 *   lock.readLock().unlock();
 * }
 * </pre>
 *
 * For write lock, use lock.writeLock()
 */
public abstract class IdReadWriteLock<T>
{
    public abstract ReentrantReadWriteLock getLock(T id);

    public void waitForWaiters(T id, int numWaiters)
            throws InterruptedException
    {
        for (ReentrantReadWriteLock readWriteLock; ; ) {
            readWriteLock = getLock(id);
            if (readWriteLock != null) {
                synchronized (readWriteLock) {
                    if (readWriteLock.getQueueLength() >= numWaiters) {
                        return;
                    }
                }
            }
            Thread.sleep(50);
        }
    }
}
