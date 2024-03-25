package pl.edu.put.occdemo.utils;

import java.util.*;
import java.util.function.*;
import java.util.stream.*;

public class MergingSpliterator<T> extends Spliterators.AbstractSpliterator<T> {

    private final Iterator<T> iterator1;
    private final Iterator<T> iterator2;
    private final Comparator<? super T> comparator;
    private T next1, next2;
    private boolean advance1 = true, advance2 = true;

    public MergingSpliterator(Stream<T> stream1, Stream<T> stream2, Comparator<? super T> comparator) {
        super(Long.MAX_VALUE, SORTED | DISTINCT | NONNULL | IMMUTABLE);
        this.iterator1 = stream1.iterator();
        this.iterator2 = stream2.iterator();
        this.comparator = comparator;
    }

    @Override
    public Comparator<? super T> getComparator() {
        return comparator;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {
        if (advance1 && next1 == null && iterator1.hasNext()) {
            next1 = iterator1.next();
            advance1 = false;
        }
        if (advance2 && next2 == null && iterator2.hasNext()) {
            next2 = iterator2.next();
            advance2 = false;
        }

        if (next1 == null && next2 == null) {
            return false;
        }

        int cmp = 0;
        if (next1 != null && next2 != null) {
            cmp = comparator.compare(next1, next2);
        }

        if (next1 != null && (next2 == null || cmp <= 0)) {
            action.accept(next1);
            next1 = null;
            advance1 = true;
            if (cmp == 0) { // If they are equal, discard next2 and advance
                next2 = null;
                advance2 = true;
            }
        } else {
            action.accept(next2);
            next2 = null;
            advance2 = true;
        }
        return true;
    }
}

