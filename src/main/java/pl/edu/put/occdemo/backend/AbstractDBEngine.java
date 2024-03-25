package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.DBEngine;
import pl.edu.put.occdemo.DBInterface;
import pl.edu.put.occdemo.IsolationLevel;

import java.util.function.Consumer;

public abstract class AbstractDBEngine<K extends Comparable<? super K>, V> implements DBEngine<K, V> {
    protected final StorageEngine<K, V> storage;
    protected final GarbageCollector<K, V> gc;
    protected long sequenceNumber = 1;

    public AbstractDBEngine(boolean mv) {
        gc = new GarbageCollector<>();
        storage = StorageEngine.getInstance(mv, gc);
    }

    private AbstractExecutor<K, V> getNewExecutor(IsolationLevel isolationLevel, long snapshot) {
        switch (isolationLevel) {
            case READ_COMMITTED:
                return new ReadCommittedExecutor<>(snapshot, storage);
            case REPEATABLE_READ:
                return new RepeatableReadExecutor<>(snapshot, storage);
            case SNAPSHOT_ISOLATION:
                return new SnapshotIsolationExecutor<>(snapshot, storage);
            case SERIALIZABLE:
                return new SerializableExecutor<>(snapshot, storage);
            case SERIALIZABLE_SNAPSHOT_ISOLATION:
                return new SerializableSnapshotIsolationExecutor<>(snapshot, storage);
            default:
                throw new UnsupportedOperationException(); // just in case
        }
    }

    @Override
    public void execute(Consumer<DBInterface<K, V>> transaction, IsolationLevel isolationLevel) {
        try (GarbageCollector<K, V>.Snapshot snapshot = gc.registerSnapshot(() -> sequenceNumber)) {
            boolean retry = true;
            while (retry) {
                var executor = getNewExecutor(isolationLevel, snapshot.seqNumber);
                try {
                    transaction.accept(executor);
                } catch (DBInterface.CommitException e) {
                    // go on
                } catch (DBInterface.RollbackException e) {
                    break;
                } catch (DBInterface.AbortException e) {
                    continue;
                }
                retry = !doCommit(executor);
                if (retry)
                    snapshot.update(sequenceNumber);
            }
        }
        gc.collect(sequenceNumber, storage);
    }

    protected abstract boolean doCommit(AbstractExecutor<K, V> executor);
}
