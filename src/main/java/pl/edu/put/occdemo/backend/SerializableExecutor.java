package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.utils.Tools;

import java.util.*;
import java.util.function.Predicate;

class SerializableExecutor<K extends Comparable<? super K>, V> extends RepeatableReadExecutor<K, V> {

    private final List<QueryDescriptor<K, V>> rangeQueries = new ArrayList<>();

    SerializableExecutor(long snapshot, StorageEngine<K, V> storage) {
        super(snapshot, storage);
    }

    @Override
    public List<DBEntry<K, V>> rangeQuery(K from, boolean fromInclusive, K to, boolean toInclusive,
                                          Predicate<Map.Entry<K, V>> predicate, Integer limit) {
        var ret = super.rangeQuery(from, fromInclusive, to, toInclusive, predicate, limit);
        var observedUpdates = new ArrayList<>(updates.subMap(from, fromInclusive, to, toInclusive).values());
        var qd = new QueryDescriptor<>(from, fromInclusive, to, toInclusive, predicate, limit, observedUpdates, ret);
        rangeQueries.add(qd);
        return ret;
    }

    @Override
    protected boolean noUpdatesAndNoValidation() {
        return updates.isEmpty() && readSet.isEmpty() && rangeQueries.isEmpty();
    }

    private boolean sameEntries(DBEntry<K, V> current, DBEntry<K, V> old) {
        return current.key.equals(old.key) && current.seqNumber == old.seqNumber;
    }

    private boolean validateRangeQuery(QueryDescriptor<K, V> qd) {
        var stream = Tools.combineSortedStreams(
                        qd.updates.stream(),
                        storage.getRange(qd.from, qd.fromInclusive, qd.to, qd.toInclusive),
                        Comparator.comparing(DBEntry<K, V>::getKey))
                .filter(e -> e.value != null && (qd.predicate == null || qd.predicate.test(e)));

        if (qd.limit != null)
            stream = stream.limit(qd.limit);

        var resIter = qd.results.iterator();
        return stream.allMatch(e -> resIter.hasNext() && sameEntries(e, resIter.next())) && !resIter.hasNext();
    }

    @Override
    protected boolean validate() {
        if (!super.validate())
            return false;

        return rangeQueries.stream().allMatch(this::validateRangeQuery);
    }

    @Override
    protected boolean validate(NavigableMap<K, DBEntry<K, V>> otherUpdates) {
        if (!super.validate(otherUpdates))
            return false;

        return rangeQueries.stream().allMatch(qd -> qd.validate(otherUpdates));
    }
}
