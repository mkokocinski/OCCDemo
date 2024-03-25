package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.utils.Tools;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class ReadCommittedExecutor<K extends Comparable<? super K>, V> extends AbstractExecutor<K, V> {

    protected final NavigableMap<K, DBEntry<K, V>> updates = new TreeMap<>();

    ReadCommittedExecutor(long snapshot, StorageEngine<K, V> storage) {
        super(snapshot, storage);
    }

    protected Optional<DBEntry<K, V>> getUpdate(K key) {
        return Optional.ofNullable(updates.get(key));
    }

    @Override
    public V read(K key) {
        return getUpdate(key)
                .orElseGet(() -> storage.get(key))
                .value;
    }

    @Override
    public void write(K key, V value) {
        updates.put(key, new DBEntry<>(key, value));
    }

    @Override
    public void remove(K key) {
        updates.put(key, new DBEntry<>(key, null));
    }

    protected void rangeQuerySideEffect(DBEntry<K, V> e) {
    }

    @Override
    public List<DBEntry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive,
                                          Predicate<Map.Entry<K, V>> predicate, Integer limit) {
        var stream = Tools.combineSortedStreams(
                        updates.subMap(from, fromInclusive, to, toInclusive).values().stream(),
                        storage.getRange(from, fromInclusive, to, toInclusive),
                        Comparator.comparing(DBEntry<K, V>::getKey))
                .filter(e -> e.value != null && (predicate == null || predicate.test(e)));

        if (limit != null)
            stream = stream.limit(limit);

        return stream
                .peek(this::rangeQuerySideEffect)
                .collect(Collectors.toUnmodifiableList());
    }

    @Override
    protected boolean noUpdatesAndNoValidation() {
        return updates.isEmpty();
    }

    @Override
    protected boolean validate() {
        return true;
    }

    @Override
    protected boolean validate(NavigableMap<K, DBEntry<K, V>> otherUpdates) {
        return true;
    }

    @Override
    protected void applyUpdates(long seqNumber) {
        for (var update : updates.values()) {
            update.seqNumber = seqNumber;
            storage.put(update);
        }
    }

    @Override
    protected NavigableMap<K, DBEntry<K, V>> getUpdates() {
        return updates;
    }
}
