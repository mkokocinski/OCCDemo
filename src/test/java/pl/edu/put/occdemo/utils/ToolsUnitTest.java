package pl.edu.put.occdemo.utils;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class ToolsUnitTest {

    private Stream<String> getACEStream() {
        return Stream.of("a", "c", "e");
    }

    private Stream<String> getBCDFStream() {
        return Stream.of("b", "c", "d", "f");
    }

    @Test
    void combineSortedStreams() {
        var expected = List.of("a", "b", "c", "d", "e", "f");

        assertEquals(
                expected,
                Tools.combineSortedStreams(getACEStream(), getBCDFStream(), String::compareTo)
                        .collect(Collectors.toList()));
        assertEquals(
                expected,
                Tools.combineSortedStreams(getBCDFStream(), getACEStream(), String::compareTo)
                        .collect(Collectors.toList()));

        var expected2 = List.of("a", "c", "e");

        assertEquals(
                expected2,
                Tools.combineSortedStreams(getACEStream(), Stream.empty(), String::compareTo)
                        .collect(Collectors.toList()));
        assertEquals(
                expected2,
                Tools.combineSortedStreams(Stream.empty(), getACEStream(), String::compareTo)
                        .collect(Collectors.toList()));
    }

    @Test
    void setsIntersect() {
        TreeSet<String> a = new TreeSet<>();
        TreeSet<String> b = new TreeSet<>();

        a.add("a");
        a.add("b");
        a.add("c");

        b.add("x");
        b.add("y");

        assertFalse(Tools.setsIntersect(a, b));
        assertFalse(Tools.setsIntersect(b, a));

        b.add("z");

        assertFalse(Tools.setsIntersect(a, b));
        assertFalse(Tools.setsIntersect(b, a));

        a.add("s");
        b.add("s");

        assertTrue(Tools.setsIntersect(a, b));
        assertTrue(Tools.setsIntersect(b, a));

        a.add("d");

        assertTrue(Tools.setsIntersect(a, b));
        assertTrue(Tools.setsIntersect(b, a));
    }
}