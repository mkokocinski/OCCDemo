package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.utils.Tools;

import java.util.*;
import java.util.function.Predicate;

class SerializableSnapshotIsolationExecutor<K extends Comparable<? super K>, V> extends SnapshotIsolationExecutor<K, V> {

    private final List<QueryDescriptor<K, V>> rangeQueries = new ArrayList<>();

    SerializableSnapshotIsolationExecutor(long snapshot, StorageEngine<K, V> storage) {
        super(snapshot, storage);
    }

    @Override
    public List<DBEntry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive,
                                          Predicate<Map.Entry<K, V>> predicate, Integer limit) {
        var ret = super.rangeQuery(from, fromInclusive, to, toInclusive, predicate, limit);
        var observedUpdates = new ArrayList<>(updates.subMap(from, fromInclusive, to, toInclusive).values());
        var qd = new QueryDescriptor<>(from, fromInclusive, to, toInclusive, predicate, limit, observedUpdates, null);
        rangeQueries.add(qd);
        return ret;
    }

    private boolean validateRangeQuery(QueryDescriptor<K, V> qd) {
        var stream = Tools.combineSortedStreams(
                        qd.updates.stream(),
                        storage.getRange(qd.from, qd.fromInclusive, qd.to, qd.toInclusive),
                        Comparator.comparing(DBEntry::getKey))
                .filter(e -> e.getValue() != null && (qd.predicate == null || qd.predicate.test(e)));

        if (qd.limit != null)
            stream = stream.limit(qd.limit);

        // did exist and pass the predicate, now doesn't exist or pass the predicate -> covered by readSet validation
        // did exist, but didn't pass the predicate, now doesn't exist -> don't care (filtered out)
        // did exist, but didn't pass the predicate, now passes -> abort! (detected by seqNumber change below)
        // didn't exist, now exists, but doesn't pass predicate -> don't care (filtered out)
        // didn't exist, now exists and passes predicate -> abort! (detected by seqNumber change)

        return stream.allMatch(e -> e.seqNumber <= snapshot);
    }

    @Override
    protected boolean validate() {
        for (var saved : readSet.values()) {
            var current = storage.get(saved.key);
            if (current.seqNumber > snapshot)
                return false;
        }
        return rangeQueries.stream().allMatch(this::validateRangeQuery);
    }

    @Override
    protected boolean validate(NavigableMap<K, DBEntry<K, V>> otherUpdates) {
        if (!super.validate(otherUpdates))
            return false;

        return rangeQueries.stream().allMatch(qd -> qd.validate(otherUpdates));
    }
}
