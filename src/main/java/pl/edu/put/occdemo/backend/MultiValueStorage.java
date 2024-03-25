package pl.edu.put.occdemo.backend;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Stream;

class MultiValueStorage<K extends Comparable<? super K>, V> implements StorageEngine<K, V> {
    private final ConcurrentSkipListMap<K, DBEntry<K, V>> index = new ConcurrentSkipListMap<>();
    private final GarbageCollector<K,V> gc;

    public MultiValueStorage(GarbageCollector<K, V> gc) {
        this.gc = gc;
    }

    private static <K extends Comparable<? super K>, V> DBEntry<K, V> getVisibleVersion(DBEntry<K, V> entry, long snapshot) {
        var key = entry.key;
        while (entry != null && entry.seqNumber > snapshot)
            entry = entry.next;
        return Objects.requireNonNullElseGet(entry, () -> new DBEntry<>(key, null, 0));
    }

    @Override
    public DBEntry<K, V> get(K key) {
        return Objects.requireNonNullElseGet(index.get(key), () -> new DBEntry<>(key, null, 0));
    }

    @Override
    public Optional<DBEntry<K, V>> get(K key, long snapshot) {
        return Optional.of(getVisibleVersion(get(key), snapshot));
    }

    @Override
    public Stream<DBEntry<K, V>> getRange(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return index.subMap(from, fromInclusive, to, toInclusive).values().stream();
    }

    @Override
    public Stream<DBEntry<K, V>> getRange(K from, boolean fromInclusive, K to, boolean toInclusive, long snapshot, boolean strict) {
        return getRange(from, fromInclusive, to, toInclusive)
                .map(e -> getVisibleVersion(e, snapshot));
    }

    @Override
    public void put(DBEntry<K, V> entry) {
        var prev = index.get(entry.key);
        entry.next = prev;
        index.put(entry.key, entry);
        if (prev != null)
            gc.registerGarbage(entry, prev);
        if (entry.value == null)
            gc.registerGarbage(null, entry);
    }

    @Override
    public void collect(GarbageCollector.TrashEntry<K, V> trash) {
        if (trash.replacementEntry != null) {
            trash.replacementEntry.next = null;
        }
        else {
            index.remove(trash.key, trash.entry);
        }
    }
}
