package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.utils.Tools;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class SnapshotIsolationExecutor<K extends Comparable<? super K>, V> extends RepeatableReadExecutor<K, V> {

    SnapshotIsolationExecutor(long snapshot, StorageEngine<K, V> storage) {
        super(snapshot, storage);
    }

    private Optional<DBEntry<K, V>> getFromReadSet(K key) {
        return Optional.ofNullable(readSet.get(key));
    }

    private Optional<DBEntry<K, V>> getFromStorageAndSave(K key) {
        var current = storage.get(key, snapshot);
        current.ifPresent(e -> readSet.put(e.key, e));
        return current;
    }

    @Override
    public V read(K key) {
        return getUpdate(key)
                .or(() -> getFromReadSet(key))
                .or(() -> getFromStorageAndSave(key))
                .orElseThrow(AbortException::new)
                .value;
    }

    private void rangeQuerySideEffect(DBEntry<K, V> e, List<DBEntry<K, V>> readSetAddition) {
        if (e.isCommitted()) { // comes from the db, not the updates
            var saved = readSet.get(e.key);
            if (saved == null) {
                //Phantom read is not blocked here
                if (e.seqNumber > snapshot)
                    throw new AbortException();
                readSetAddition.add(e);
            }
        }
    }

    @Override
    public List<DBEntry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive,
                                          Predicate<Map.Entry<K, V>> predicate, Integer limit) {
        var updatesAndReadSetStream = Tools.combineSortedStreams(
                updates.subMap(from, fromInclusive, to, toInclusive).values().stream(),
                readSet.subMap(from, fromInclusive, to, toInclusive).values().stream(),
                Comparator.comparing(DBEntry<K, V>::getKey));
        var stream = Tools.combineSortedStreams(
                        updatesAndReadSetStream,
                        storage.getRange(from, fromInclusive, to, toInclusive, snapshot, false),
                        Comparator.comparing(DBEntry<K, V>::getKey))
                .filter(e -> e.value != null && (predicate == null || predicate.test(e)));

        if (limit != null)
            stream = stream.limit(limit);

        List<DBEntry<K, V>> readSetAddition = new ArrayList<>();
        List<DBEntry<K, V>> ret = stream
                .peek(e -> rangeQuerySideEffect(e, readSetAddition))
                .collect(Collectors.toUnmodifiableList());

        readSetAddition.forEach(e -> readSet.put(e.key, e));

        return ret;
    }

    @Override
    protected boolean noUpdatesAndNoValidation() {
        return updates.isEmpty();
    }

    @Override
    protected boolean validate() {
        for (var update : updates.values()) {
            var current = storage.get(update.key);
            if (current.seqNumber > snapshot)
                return false;
        }
        return true;
    }

    @Override
    protected boolean validate(NavigableMap<K, DBEntry<K, V>> otherUpdates) {
        return !Tools.setsIntersect(updates.navigableKeySet(), otherUpdates.navigableKeySet());
    }
}
