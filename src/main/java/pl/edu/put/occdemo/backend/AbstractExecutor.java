package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.DBInterface;

import java.util.NavigableMap;

abstract class AbstractExecutor<K extends Comparable<? super K>, V> implements DBInterface<K, V> {

    final protected long snapshot;
    final protected StorageEngine<K, V> storage;

    AbstractExecutor(long snapshot, StorageEngine<K, V> storage) {
        this.snapshot = snapshot;
        this.storage = storage;
    }

    protected abstract boolean noUpdatesAndNoValidation();

    protected abstract boolean validate();

    protected abstract boolean validate(NavigableMap<K, DBEntry<K, V>> otherUpdates);

    protected abstract void applyUpdates(long seqNumber);

    protected abstract NavigableMap<K, DBEntry<K, V>> getUpdates();
}
