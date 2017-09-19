# concurrency
Useful concurrency stuff

1. Locker

Useful in applications that manage group of entities (such as pools, storages, caches) distinguished by some key or id.
Provides exclusive access to individual entities in such application. Features:
1. Reentrant locks
2. Lock timeout
3. Try lock
4. Global lock on the whole entities group
5. Optional deadlock protection
6. Optional lock escalation to global if thread acquires locks on more then N entities

Implementation notes: simple implementation - uses locks from java.util.concurrent.locks package not extending AbstractQueuedSynchronizer