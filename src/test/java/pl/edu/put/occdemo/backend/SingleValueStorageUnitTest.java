package pl.edu.put.occdemo.backend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class SingleValueStorageUnitTest {

    final long SEQ_NUMBER = 5;
    @Mock GarbageCollector<String, String> gc;
    SingleValueStorage<String, String> storage;
    DBEntry<String, String> entryB0 = new DBEntry<>("b", null, 0);
    DBEntry<String, String> entryD0 = new DBEntry<>("d", null, 0);
    DBEntry<String, String> entryA = new DBEntry<>("a", "1", SEQ_NUMBER);
    DBEntry<String, String> entryC = new DBEntry<>("c", "3", SEQ_NUMBER);
    DBEntry<String, String> entryD = new DBEntry<>("d", "4", SEQ_NUMBER);
    DBEntry<String, String> entryE = new DBEntry<>("e", "5", SEQ_NUMBER);

    @BeforeEach
    void setUp() {
        storage = new SingleValueStorage<>(gc);
        storage.put(entryA);
        storage.put(entryC);
        storage.put(entryD);
        storage.put(entryE);
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(gc);
    }

    @Test
    void get() {
        assertEquals(entryA, storage.get("a"));
        assertEquals(entryB0, storage.get("b"));
        assertEquals(entryC, storage.get("c"));
        assertEquals(entryD, storage.get("d"));
        assertEquals(entryE, storage.get("e"));
    }

    @Test
    void getWithSnapshot() {
        assertEquals(Optional.of(entryA), storage.get("a", SEQ_NUMBER));
        assertEquals(Optional.of(entryB0), storage.get("b", SEQ_NUMBER));
        assertEquals(Optional.of(entryC), storage.get("c", SEQ_NUMBER));
        assertEquals(Optional.of(entryD), storage.get("d", SEQ_NUMBER));
        assertEquals(Optional.of(entryE), storage.get("e", SEQ_NUMBER));

        assertEquals(Optional.empty(), storage.get("a", SEQ_NUMBER - 1));
        assertEquals(Optional.of(entryB0), storage.get("b", SEQ_NUMBER - 1));
        assertEquals(Optional.empty(), storage.get("c", SEQ_NUMBER - 1));
        assertEquals(Optional.empty(), storage.get("d", SEQ_NUMBER - 1));
        assertEquals(Optional.empty(), storage.get("e", SEQ_NUMBER - 1));
    }

    @Test
    void getRange() {
        assertEquals(
                List.of(entryA, entryC, entryD, entryE),
                storage.getRange("a", true, "e", true)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryA, entryC, entryD),
                storage.getRange("a", true, "e", false)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryC, entryD, entryE),
                storage.getRange("a", false, "e", true)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryC, entryD),
                storage.getRange("a", false, "e", false)
                        .collect(Collectors.toList()));
    }

    private void testSnaphotRanges(long seqNumber) {
        assertEquals(
                List.of(entryA, entryC, entryD, entryE),
                storage.getRange("a", true, "e", true, seqNumber, false)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryA, entryC, entryD),
                storage.getRange("a", true, "e", false, seqNumber, false)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryC, entryD, entryE),
                storage.getRange("a", false, "e", true, seqNumber, false)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryC, entryD),
                storage.getRange("a", false, "e", false, seqNumber, false)
                        .collect(Collectors.toList()));
    }

    @Test
    void getRangeWithSnapshot() {
        testSnaphotRanges(SEQ_NUMBER);
        testSnaphotRanges(SEQ_NUMBER - 1);
        assertThrows(UnsupportedOperationException.class, () ->
                storage.getRange("a", true, "e", true, SEQ_NUMBER, true));
    }

    @Test
    void put() {
        var entryB = new DBEntry<>("b", "2", SEQ_NUMBER + 1);
        var entryC2 = new DBEntry<>("c", "33", SEQ_NUMBER + 1);
        var entryD2 = new DBEntry<String, String>("d", null, SEQ_NUMBER + 1);
        storage.put(entryB);
        storage.put(entryC2);
        storage.put(entryD2);

        assertEquals(entryB, storage.get("b"));
        assertEquals(Optional.of(entryB), storage.get("b", SEQ_NUMBER + 1));
        assertEquals(Optional.empty(), storage.get("b", SEQ_NUMBER));

        assertEquals(entryC2, storage.get("c"));
        assertEquals(Optional.of(entryC2), storage.get("c", SEQ_NUMBER + 1));
        assertEquals(Optional.empty(), storage.get("c", SEQ_NUMBER));

        assertEquals(entryD2, storage.get("d"));
        assertEquals(Optional.of(entryD2), storage.get("d", SEQ_NUMBER + 1));
        assertEquals(Optional.empty(), storage.get("d", SEQ_NUMBER));

        assertEquals(
                List.of(entryA, entryB, entryC2, entryD2, entryE),
                storage.getRange("a", true, "e", true)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryA, entryB, entryC2, entryD2, entryE),
                storage.getRange("a", true, "e", true, SEQ_NUMBER + 1, false)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryA, entryB, entryC2, entryD2, entryE),
                storage.getRange("a", true, "e", true, SEQ_NUMBER, false)
                        .collect(Collectors.toList()));

        verify(gc).registerGarbage(null, entryD2);
    }

    @Test
    void collect() {
        var entryD2 = new DBEntry<String, String>("d", null, SEQ_NUMBER + 1);
        storage.put(entryD2);

        storage.collect(new GarbageCollector.TrashEntry<>(null, entryD2));

        assertEquals(entryD0, storage.get("d"));
        assertEquals(Optional.of(entryD0), storage.get("d", SEQ_NUMBER + 1));
        assertEquals(Optional.of(entryD0), storage.get("d", SEQ_NUMBER));

        assertEquals(
                List.of(entryA, entryC, entryE),
                storage.getRange("a", true, "e", true)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryA, entryC, entryE),
                storage.getRange("a", true, "e", true, SEQ_NUMBER + 1, false)
                        .collect(Collectors.toList()));
        assertEquals(
                List.of(entryA, entryC, entryE),
                storage.getRange("a", true, "e", true, SEQ_NUMBER, false)
                        .collect(Collectors.toList()));

        verify(gc).registerGarbage(null, entryD2);
    }
}