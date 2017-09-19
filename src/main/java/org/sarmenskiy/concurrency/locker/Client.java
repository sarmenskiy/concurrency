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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Tests
 *
 * @author sarmenskiy
 * created: 18.06.2017
 */
public class Client {
    public static void main(String[] args) throws InterruptedException {
        testLockEscalation();
        testDeadlockProtection();
    }

    private static void testLockEscalation() throws InterruptedException {
        System.out.println("\n*** testLockEscalation ***");
        final Locker<String> locker = new Locker<>(2, null);
        locker.lock("a");
        locker.lock("b");
        locker.lock(new String("a"));
        assert !locker.isGlobalLockByCurrentThread();
        locker.tryLock("c", 1, TimeUnit.SECONDS);
        assert locker.isGlobalLockByCurrentThread();
        locker.unlock("c");
        locker.unlock("b");
        Thread th1 = new Thread(() -> {
            try {
                // should fail since main thread holds global lock
                assert !locker.tryLock("x", 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("interruption is not expected", e);
            }
        });
        th1.start();
        th1.join();
        locker.unlock("a");
        locker.unlock("a");
        assert !locker.isGlobalLockByCurrentThread();
    }

    private static void testDeadlockProtection() {
        System.out.println("\n*** testDeadlockProtection ***");
        final Locker<String> locker = new Locker<>(
                Integer.MAX_VALUE, (String s1, String s2) -> { return s1.compareTo(s2); });
        final CountDownLatch latch = new CountDownLatch(2);
        new Thread(() -> {
            boolean invalidLockOrder = false;
            locker.lock("b");
            latch.countDown();
            try {
                latch.await();
                try {
                    locker.lock("a");
                } catch (Locker.DeadlockProtectionException e) {
                    invalidLockOrder = true;
                }
                assert invalidLockOrder;
            } catch (InterruptedException e) {
                throw new RuntimeException("interruption is not expected", e);
            }
        }).start();
        new Thread(() -> {
            locker.lock("a");
            latch.countDown();
            try {
                latch.await();
                assert !locker.tryLock("b", 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                throw new RuntimeException("interruption is not expected", e);
            }
        }).start();
    }
}
