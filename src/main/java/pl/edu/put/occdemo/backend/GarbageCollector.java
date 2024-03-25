package pl.edu.put.occdemo.backend;

import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.LongSupplier;
import java.util.stream.Stream;

class GarbageCollector<K extends Comparable<? super K>, V> {

    private final CopyOnWriteArrayList<Snapshot> snapshots = new CopyOnWriteArrayList<>();
    private final NavigableMap<TrashEntry<K, V>, TrashEntry<K, V>> trashBin = new ConcurrentSkipListMap<>();

    public Snapshot registerSnapshot(LongSupplier seqVersionSupplier) {
        var s = new Snapshot(seqVersionSupplier.getAsLong());
        snapshots.add(s);
        s.update(seqVersionSupplier.getAsLong());
        return s;
    }

    private void unregisterSnapshot(Snapshot snapshot) {
        boolean res = snapshots.remove(snapshot);
        assert res;
    }

    public void registerGarbage(DBEntry<K, V> replacementEntry, DBEntry<K, V> entry) {
        var trash = new TrashEntry<>(replacementEntry, entry);
        trashBin.put(trash, trash);
    }

    public void collect(long currentSeqNumber, StorageEngine<K, V> storage) {
        getTrash(currentSeqNumber).forEach(storage::collect);
    }

    private Stream<TrashEntry<K, V>> getTrash(long currentSeqNumber) {
        long min = snapshots.stream().mapToLong(e -> e.seqNumber).min().orElse(currentSeqNumber);

        return Stream.generate(() -> getNextTrashEntry(min))
                .takeWhile(Objects::nonNull);
    }

    private TrashEntry<K, V> getNextTrashEntry(long min) {
        while (true) {
            var entry = trashBin.firstEntry();
            if (entry == null || entry.getValue().seqNumber >= min)
                return null;
            if (trashBin.remove(entry.getValue()) == null)
                continue;
            return entry.getValue();
        }
    }

    static class TrashEntry<K extends Comparable<? super K>, V> implements Comparable<TrashEntry<K, V>> {
        long seqNumber;
        K key;
        DBEntry<K, V> replacementEntry;
        DBEntry<K, V> entry;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TrashEntry<?, ?> that = (TrashEntry<?, ?>) o;
            return seqNumber == that.seqNumber && key.equals(that.key) && Objects.equals(replacementEntry, that.replacementEntry) && entry.equals(that.entry);
        }

        @Override
        public int hashCode() {
            return Objects.hash(seqNumber, key, replacementEntry, entry);
        }

        @Override
        public String toString() {
            return "TrashEntry{" +
                    "seqNumber=" + seqNumber +
                    ", key=" + key +
                    ", replacementEntry=" + replacementEntry +
                    ", entry=" + entry +
                    '}';
        }

        public TrashEntry(DBEntry<K, V> replacementEntry, DBEntry<K, V> entry) {
            this.seqNumber = entry.seqNumber;
            this.key = entry.key;
            this.replacementEntry = replacementEntry;
            this.entry = entry;
        }

        @Override
        public int compareTo(TrashEntry<K, V> o) {
            int res = Long.compare(seqNumber, o.seqNumber);
            if (res != 0)
                return res;
            return key.compareTo(o.key);
        }
    }

    class Snapshot implements AutoCloseable {
        volatile long seqNumber;

        Snapshot(long seqNumber) {
            this.seqNumber = seqNumber;
        }

        void update(long seqNumber) {
            assert seqNumber >= this.seqNumber;
            this.seqNumber = seqNumber;
        }

        @Override
        public void close() {
            unregisterSnapshot(this);
        }
    }
}
