package pl.edu.put.occdemo.utils;

import java.util.Comparator;
import java.util.NavigableSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class Tools {
    public static <E> Stream<E> combineSortedStreams(Stream<E> stream1, Stream<E> stream2, Comparator<E> comparator) {
        return StreamSupport.stream(new MergingSpliterator<>(stream1, stream2, comparator), false);
    }

    public static <E> boolean setsIntersect(NavigableSet<E> a, NavigableSet<E> b) {
        if (a.size() < b.size())
            return a.stream().anyMatch(e -> b.contains(e));
        else
            return b.stream().anyMatch(e -> a.contains(e));
    }
}
