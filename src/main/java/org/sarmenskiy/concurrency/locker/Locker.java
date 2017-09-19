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

import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Locker is useful in applications that manage group of entities (such as pools, storages, caches) distinguished by some key or id.
 * Provides exclusive access to individual entities in such application. Features:
 * 1. Reentrant locks
 * 2. Lock timeout
 * 3. Try lock
 * 4. Global lock on the whole entities group
 * 5. Optional deadlock protection
 * 6. Optional lock escalation to global if thread acquires locks on more then N entities
 *
 * Note that runtime exception {@link DeadlockProtectionException} is thrown in deadlock protection mode in @lock method
 * when trying to lock entity currently held by other thread in order different from supplied Comparator.
 * Use @tryLock with timeout for such out-of-order locking.
 *
 * @author sarmenskiy
 * created: 17.06.2017
 */
public class Locker<T> {

    private final int locksLimit;
    private final Comparator comparator;

    static boolean verbose = true;

    private final LockStore<T> lockStore = new LockStore<>();
    private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();

    private final ThreadLocal<Integer> readLocksCount = ThreadLocal.withInitial(() -> 0);
    private final ThreadLocal<LinkedHashSet<T>> lockedEntities = ThreadLocal.withInitial(LinkedHashSet::new);
    private final ThreadLocal<Boolean> escalation = ThreadLocal.withInitial(() -> false);

    public Locker() {
        this(Integer.MAX_VALUE, null);
    }

    /**
     * @param locksLimit the number of locked entities to escalate to global lock on next entity lock call,
     *                   or Integer.MAX_VALUE if no escalation required
     * @param comparator deadlock protection mode lock order Comparator
     *                   or null to disable deadlock protection
     */
    public Locker(int locksLimit, Comparator<T> comparator) {
        this.locksLimit = locksLimit;
        this.comparator = comparator;
    }

    public void lockGlobally() {
        removeReadLocks(); // ReentrantReadWriteLock doesn't support read->write lock upgrade
        log("lockGlobally...");
        globalLock.writeLock().lock();
    }

    /**
     * @param timeout the time to wait for the lock
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the lock was free and was acquired by the
     *         current thread, or the lock was already held by the current
     *         thread; and {@code false} if the waiting time elapsed before
     *         the lock could be acquired
     * @throws InterruptedException if the current thread is interrupted
     * @throws NullPointerException if the time unit is null
     */
    public boolean tryLockGlobally(long timeout, TimeUnit unit) throws InterruptedException {
        removeReadLocks(); // remove all read locks held by current thread since read->write lock upgrade not supported
        log("tryLockGlobally...");
        return globalLock.writeLock().tryLock(timeout, unit);
    }

    public void unlockGlobally() {
        if (!globalLock.writeLock().isHeldByCurrentThread()) {
            log("unlockGlobally: thread doesn't hold global lock");
            return;
        }
        log("unlockGlobally");
        globalLock.writeLock().unlock();
    }

    /**
     * @param entityId entity ID
     * @throws DeadlockProtectionException in deadlock protection mode if out-of-order locking detected
     */
    public void lock(T entityId) {
        if (entityId == null || globalLock.writeLock().isHeldByCurrentThread()) {
            log("lock " + entityId + ": holds global lock, skipping");
            return;
        }
        deadlockProtectionCheck(entityId);
        log("lock " + entityId);
        if (shouldEscalate(entityId)) {
            escalation.set(true);
            lockGlobally();
        } else {
            readLocksCount.set(readLocksCount.get() + 1);
            globalLock.readLock().lock();
            lockStore.getOrCreateLock(entityId).lock();
            lockedEntities.get().add(entityId);
            log(entityId + " locked");
        }
    }

    /**
     * @param entityId entity ID
     * @param timeout the time to wait for the lock
     * @param unit the time unit of the timeout argument
     * @return {@code true} if the lock was free and was acquired by the
     *         current thread, or the lock was already held by the current
     *         thread; or lock was successfully escalated to global lock;
     *         and {@code false} if the waiting time elapsed before
     *         the lock could be acquired
     * @throws InterruptedException if the current thread is interrupted
     * @throws NullPointerException if the time unit is null
     */
    public boolean tryLock(T entityId, long timeout, TimeUnit unit) throws InterruptedException {
        if (entityId == null || globalLock.writeLock().isHeldByCurrentThread()) {
            log("tryLock " + entityId + ": holds global lock, skipping");
            return false;
        }
        log("tryLock " + entityId);
        if (shouldEscalate(entityId)) {
            escalation.set(true);
            return tryLockGlobally(timeout, unit);
        } else if (globalLock.readLock().tryLock(timeout, unit)) {
            readLocksCount.set(readLocksCount.get() + 1);
            try {
                if (lockStore.getOrCreateLock(entityId).tryLock(timeout, unit)) {
                    lockedEntities.get().add(entityId);
                    log(entityId + " locked");
                    return true;
                }
                lockStore.cleanupLock(entityId);
                removeReadLock();
                return false;
            } catch (InterruptedException ex) {
                lockStore.cleanupLock(entityId);
                removeReadLock();
                throw ex;
            }
        }
        return false;
    }

    /**
     * @param entityId entity ID
     */
    public void unlock(T entityId) {
        if (entityId == null) {
            return;
        }
        log("unlock: " + entityId);
        ReentrantLock lock = lockStore.getLock(entityId);
        if (lock != null && lock.isHeldByCurrentThread()) {
            lockStore.cleanupLock(entityId);
            lock.unlock();
            removeReadLock();
            log("unlock " + entityId + ": done");
            if (!lock.isHeldByCurrentThread()) {
                lockedEntities.get().remove(entityId);
                log("unlock " + entityId + ": locked entities=" + lockedEntities.get());
                if (escalation.get() && lockedEntities.get().isEmpty()) {
                    log("escalation completed");
                    escalation.set(false);
                    unlockGlobally();
                }
            }
        }
    }

    /**
     * @param entityId entity ID
     * @return true if locked by current thread
     */
    public boolean isLockedByCurrentThread(T entityId) {
        if (isGlobalLockByCurrentThread()) {
            return true;
        }
        ReentrantLock lock = lockStore.getLock(entityId);
        return lock != null && lock.isHeldByCurrentThread();
    }

    /**
     * @return true if current thread holds global lock
     */
    public boolean isGlobalLockByCurrentThread() {
        return globalLock.writeLock().isHeldByCurrentThread();
    }

    private boolean shouldEscalate(T entityId) {
        if (lockedEntities.get().contains(entityId) || lockedEntities.get().size() < locksLimit) {
            return false;
        }
        log("shouldEscalate=true");
        return true;
    }

    private void removeReadLock() {
        Integer cnt = readLocksCount.get();
        if (cnt > 0) {
            globalLock.readLock().unlock();
            readLocksCount.set(cnt - 1);
        }
    }

    private void removeReadLocks() {
        for (int i = 0; i < readLocksCount.get(); i++) {
            globalLock.readLock().unlock();
        }
        readLocksCount.set(0);
    }

    private void deadlockProtectionCheck(T entityId) {
        if (comparator != null && !lockedEntities.get().contains(entityId) && lockStore.getLock(entityId) != null) { // locked by other thread
            T lastLocked = null;
            for (Iterator<T> it = lockedEntities.get().iterator(); it.hasNext(); ) {
                lastLocked = it.next();
            }
            if (lastLocked != null && comparator.compare(lastLocked, entityId) > 0) {
                log("DeadlockProtectionException " + entityId);
                throw new DeadlockProtectionException(
                        "Use tryLock for out-of-order locking of entity " + entityId + " currently held by other thread");
            }
        }
    }

    static void log(String msg) {
        if (verbose) {
            System.out.println(Thread.currentThread().getName() + ": " + msg);
        }
    }


    public static class DeadlockProtectionException extends RuntimeException {
        DeadlockProtectionException(String msg) {
            super(msg);
        }
    }
}
