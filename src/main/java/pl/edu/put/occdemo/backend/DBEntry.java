package pl.edu.put.occdemo.backend;

import java.util.Map;
import java.util.Objects;

class DBEntry<K extends Comparable<? super K>, V> implements Map.Entry<K, V> {
    long seqNumber;
    K key;
    V value;
    volatile DBEntry<K, V> next;

    public DBEntry(K key, V value) {
        this(key, value, -1);
    }

    public DBEntry(K key, V value, long seqNumber) {
        this(key, value, seqNumber, null);
    }

    public DBEntry(K key, V value, long seqNumber, DBEntry<K, V> next) {
        this.key = key;
        this.value = value;
        this.seqNumber = seqNumber;
        this.next = next;
    }

    public boolean isCommitted() {
        return seqNumber >= 0;
    }

    @Override
    public K getKey() {
        return key;
    }

    @Override
    public V getValue() {
        return value;
    }

    @Override
    public V setValue(V value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DBEntry<?, ?> entry = (DBEntry<?, ?>) o;
        return Objects.equals(key, entry.key) &&
                Objects.equals(value, entry.value) &&
                seqNumber == entry.seqNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return "DBEntry{" +
                "key=" + key +
                ", value=" + value +
                ", seqNumber=" + seqNumber +
                ", next=" + next +
                '}';
    }
}
