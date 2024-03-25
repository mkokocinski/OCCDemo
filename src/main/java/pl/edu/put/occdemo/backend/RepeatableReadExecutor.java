package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.utils.Tools;

import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;

class RepeatableReadExecutor<K extends Comparable<? super K>, V> extends ReadCommittedExecutor<K, V> {

    protected final NavigableMap<K, DBEntry<K, V>> readSet = new TreeMap<>();

    RepeatableReadExecutor(long snapshot, StorageEngine<K, V> storage) {
        super(snapshot, storage);
    }

    private Optional<DBEntry<K, V>> validateAndSave(DBEntry<K, V> current) {
        var saved = readSet.get(current.key);
        if (saved == null) {
            readSet.put(current.key, current);
        } else if (saved.seqNumber != current.seqNumber) {
            return Optional.empty();
        }
        return Optional.of(current);
    }

    private Optional<DBEntry<K, V>> getFromStorageAndSave(K key) {
        return validateAndSave(storage.get(key));
    }

    @Override
    public V read(K key) {
        return getUpdate(key)
                .or(() -> getFromStorageAndSave(key))
                .orElseThrow(AbortException::new)
                .value;
    }

    @Override
    protected void rangeQuerySideEffect(DBEntry<K, V> e) {
        if (e.isCommitted()) { // comes from the db, not the updates
            validateAndSave(e).orElseThrow(AbortException::new);
        }
    }

    @Override
    protected boolean validate() {
        for (var saved : readSet.values()) {
            var current = storage.get(saved.key);
            if (saved.seqNumber != current.seqNumber)
                return false;
        }
        return true;
    }

    @Override
    protected boolean validate(NavigableMap<K, DBEntry<K, V>> otherUpdates) {
        return !Tools.setsIntersect(readSet.navigableKeySet(), otherUpdates.navigableKeySet());
    }

    @Override
    protected boolean noUpdatesAndNoValidation() {
        return updates.isEmpty() && readSet.isEmpty();
    }
}
