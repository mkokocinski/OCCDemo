package pl.edu.put.occdemo.backend;

import java.util.*;
import java.util.function.Predicate;

class QueryDescriptor<K extends Comparable<? super K>, V> {
    K from;
    boolean fromInclusive;
    K to;
    boolean toInclusive;
    Predicate<Map.Entry<K, V>> predicate;
    Integer limit;
    List<DBEntry<K, V>> updates;
    List<DBEntry<K, V>> results;

    public QueryDescriptor(K from, boolean fromInclusive, K to, boolean toInclusive,
                           Predicate<Map.Entry<K, V>> predicate, Integer limit, List<DBEntry<K, V>> updates,
                           List<DBEntry<K, V>> results) {
        this.from = from;
        this.fromInclusive = fromInclusive;
        this.to = to;
        this.toInclusive = toInclusive;
        this.predicate = predicate;
        this.limit = limit;
        this.updates = updates;
        this.results = results;
    }

    // Used by SerializableExecutor and SerializableSnapshotIsolationExecutor for validating queries
    // against a set of concurrent updates, after readSet validation has been completed.
    public boolean validate(NavigableMap<K, DBEntry<K, V>> otherUpdates) {
        boolean limitReached = results.size() == limit;
        K to = limitReached ? results.get(results.size() - 1).key : this.to;
        boolean toInclusive = limitReached || this.toInclusive;

        // if update.key in qd.updates -> ignore
        // if update.key in results -> conflict (covered by readSet validation)
        // if update.key not in results and value != null and predicate.test(e) -> conflict
        return otherUpdates.subMap(from, fromInclusive, to, toInclusive).values().stream()
                .filter(e -> Collections.binarySearch(updates, e, Comparator.comparing(DBEntry<K, V>::getKey)) < 0) // not in qd.updates
                .noneMatch(e -> e.value != null && (predicate == null || predicate.test(e)));
    }

}
