package pl.edu.put.occdemo.backend;

import java.util.Optional;
import java.util.stream.Stream;

interface StorageEngine<K extends Comparable<? super K>, V> {
    static <K extends Comparable<? super K>, V> StorageEngine<K, V> getInstance(boolean mv, GarbageCollector<K, V> gc) {
        if (mv)
            return new MultiValueStorage<>(gc);
        else
            return new SingleValueStorage<>(gc);
    }

    DBEntry<K, V> get(K key);

    Optional<DBEntry<K, V>> get(K key, long snapshot);

    Stream<DBEntry<K, V>> getRange(K from, boolean fromInclusive, K to, boolean toInclusive);

    // If possible return only entries whose seqNumber <= snapshot.
    // When it's not possible to satisfy the snapshot constraint:
    // if strict == false, the implementation may return entries whose seqNumber > snapshot,
    // if strict == true, an UnsupportedOperationException is thrown.
    Stream<DBEntry<K, V>> getRange(K from, boolean fromInclusive, K to, boolean toInclusive, long snapshot, boolean strict);

    void put(DBEntry<K, V> entry);

    void collect(GarbageCollector.TrashEntry<K, V> trash);
}
