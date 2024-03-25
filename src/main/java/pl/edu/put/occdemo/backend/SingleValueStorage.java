package pl.edu.put.occdemo.backend;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

class SingleValueStorage<K extends Comparable<? super K>, V> implements StorageEngine<K, V> {
    private final ConcurrentSkipListMap<K, DBEntry<K, V>> index = new ConcurrentSkipListMap<>();
    private final GarbageCollector<K, V> gc;

    public SingleValueStorage(GarbageCollector<K, V> gc) {
        this.gc = gc;
    }

    @Override
    public DBEntry<K, V> get(K key) {
        return Objects.requireNonNullElseGet(index.get(key), () -> new DBEntry<>(key, null, 0));
    }

    @Override
    public Optional<DBEntry<K, V>> get(K key, long snapshot) {
        var entry = get(key);
        if (entry.seqNumber > snapshot)
            return Optional.empty();
        return Optional.of(entry);
    }

    @Override
    public Stream<DBEntry<K, V>> getRange(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return index.subMap(from, fromInclusive, to, toInclusive).values().stream();
    }

    @Override
    public Stream<DBEntry<K, V>> getRange(K from, boolean fromInclusive, K to, boolean toInclusive, long snapshot, boolean strict) {
        if (strict)
            throw new UnsupportedOperationException();
        return getRange(from, fromInclusive, to, toInclusive);
    }

    @Override
    public void put(DBEntry<K, V> entry) {
        index.put(entry.key, entry);
        if (entry.value == null)
            gc.registerGarbage(null, entry);
    }

    @Override
    public void collect(GarbageCollector.TrashEntry<K, V> trash) {
        assert trash.replacementEntry == null;
        index.remove(trash.key, trash.entry);
    }
}
