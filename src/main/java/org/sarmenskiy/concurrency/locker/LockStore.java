/**
 * Copyright [2017] [Sergey Armenskiy]
 * 
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
package org.sarmenskiy.concurrency.locker;

import java.util.HashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Lock store with cleanup support
 *
 * @author sarmenskiy
 * created: 18.06.2017
 */
class LockStore<T> {
    private final HashMap<T, CountingReentrantLock> id2lock = new HashMap<>();

    /**
     * Returns lock and increments lock usage counter
     * @param entityId entity ID
     * @return lock
     */
    ReentrantLock getOrCreateLock(T entityId) {
        synchronized (id2lock) {
            if (!id2lock.containsKey(entityId)) {
                id2lock.put(entityId, new CountingReentrantLock());
            }
            CountingReentrantLock lock = id2lock.get(entityId);
            lock.count++;
            return lock;
        }
    }

    /**
     * Returns lock not incrementing usage counter
     * @param entityId entity ID
     * @return lock
     */
    ReentrantLock getLock(T entityId) {
        synchronized (id2lock) {
            return id2lock.get(entityId);
        }
    }

    /**
     * Decrements lock usage counter and removes lock if it's not used any more
     * @param entityId entity ID
     */
    void cleanupLock(T entityId) {
        synchronized (id2lock) {
            CountingReentrantLock lock = id2lock.get(entityId);
            if (lock == null) {
                return;
            }
            lock.count--;
            if (lock.count == 0) {
                id2lock.remove(entityId);
                Locker.log("store cleanup: " + entityId);
            }
        }
    }


    private static class CountingReentrantLock extends ReentrantLock {
        private int count = 0;
    }
}
