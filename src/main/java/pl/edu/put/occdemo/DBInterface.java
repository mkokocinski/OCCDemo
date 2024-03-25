package pl.edu.put.occdemo;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public interface DBInterface<K extends Comparable<? super K>, V> {
    V read(K key);

    // Upsert semantics akin to:
    // MySQL's and MariaDB's INSERT ... ON DUPLICATE KEY UPDATE
    // PostgreSQL's: INSERT ... ON CONFLICT ... DO UPDATE
    // SQLite's: INSERT OR REPLACE
    // SQL Server's: MERGE
    void write(K key, V value);

    void remove(K key);

    default List<? extends Map.Entry<K, V>> rangeQuery(K from, K to) {
        return rangeQuery(from, true, to, false, null, null);
    }

    default List<? extends Map.Entry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive) {
        return rangeQuery(from, fromInclusive, to, toInclusive, null, null);
    }

    default List<? extends Map.Entry<K, V>> rangeQuery(K from, K to, Predicate<Map.Entry<K, V>> predicate) {
        return rangeQuery(from, true, to, false, predicate, null);
    }

    default List<? extends Map.Entry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive, Predicate<Map.Entry<K, V>> predicate) {
        return rangeQuery(from, fromInclusive, to, toInclusive, predicate, null);
    }

    default List<? extends Map.Entry<K, V>> rangeQuery(K from, K to, Integer limit) {
        return rangeQuery(from, true, to, false, null, limit);
    }

    default List<? extends Map.Entry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive, Integer limit) {
        return rangeQuery(from, fromInclusive, to, toInclusive, null, limit);
    }

    default List<? extends Map.Entry<K, V>> rangeQuery(K from, K to, Predicate<Map.Entry<K, V>> predicate, Integer limit) {
        return rangeQuery(from, true, to, false, predicate, limit);
    }

    List<? extends Map.Entry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive, Predicate<Map.Entry<K, V>> predicate, Integer limit);

    default void commit() {
        throw new CommitException();
    }

    default void rollback() {
        throw new RollbackException();
    }

    default void retry() {
        throw new AbortException();
    }

    class ControlException extends RuntimeException {
    }

    class CommitException extends ControlException {
    }

    class RollbackException extends ControlException {
    }

    class AbortException extends ControlException {
    }
}
