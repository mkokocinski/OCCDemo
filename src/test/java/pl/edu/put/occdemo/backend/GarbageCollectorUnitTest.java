package pl.edu.put.occdemo.backend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

@ExtendWith(MockitoExtension.class)
class GarbageCollectorUnitTest {
    final long SEQ_NUMBER = 10;
    GarbageCollector<String, String> gc;
    @Mock
    StorageEngine<String, String> storage;
    DBEntry<String, String> entryC = new DBEntry<>("c", "3", SEQ_NUMBER);
    DBEntry<String, String> entryD = new DBEntry<>("d", "4", SEQ_NUMBER);
    DBEntry<String, String> entryC2 = new DBEntry<>("c", "33", SEQ_NUMBER + 1);
    DBEntry<String, String> entryD2 = new DBEntry<String, String>("d", null, SEQ_NUMBER + 1);

    @BeforeEach
    void setUp() {
        gc = new GarbageCollector<>();
    }

    @AfterEach
    void tearDown() {
        verifyNoMoreInteractions(storage);
    }

    @Test
    void registerSnapshot() {
        try (var s = gc.registerSnapshot(() -> SEQ_NUMBER)) {
            assertEquals(SEQ_NUMBER, s.seqNumber);
        }
    }

    @Test
    void registerGarbage() {
        // Just testing whether no exceptions are thrown
        gc.registerGarbage(entryC2, entryC);
        gc.registerGarbage(entryD2, entryD);
        gc.registerGarbage(null, entryD2);
    }

    @Test
    void collect1() {
        registerGarbage();
        gc.collect(SEQ_NUMBER, storage);
    }

    @Test
    void collect2() {
        registerGarbage();
        gc.collect(SEQ_NUMBER + 1, storage);

        verify(storage).collect(new GarbageCollector.TrashEntry<>(entryC2, entryC));
        verify(storage).collect(new GarbageCollector.TrashEntry<>(entryD2, entryD));
    }

    @Test
    void collect3() {
        registerGarbage();
        gc.collect(SEQ_NUMBER + 2, storage);

        verify(storage).collect(new GarbageCollector.TrashEntry<>(entryC2, entryC));
        verify(storage).collect(new GarbageCollector.TrashEntry<>(entryD2, entryD));
        verify(storage).collect(new GarbageCollector.TrashEntry<>(null, entryD2));
    }

    @Test
    void collect4() {
        registerGarbage();
        gc.collect(SEQ_NUMBER + 1, storage);
        gc.collect(SEQ_NUMBER + 2, storage);

        verify(storage).collect(new GarbageCollector.TrashEntry<>(entryC2, entryC));
        verify(storage).collect(new GarbageCollector.TrashEntry<>(entryD2, entryD));
        verify(storage).collect(new GarbageCollector.TrashEntry<>(null, entryD2));
    }

    @Test
    void collectWithSnapshot1() {
        try (var s = gc.registerSnapshot(() -> SEQ_NUMBER + 1)) {
            registerGarbage();
            gc.collect(SEQ_NUMBER + 2, storage);

            verify(storage).collect(new GarbageCollector.TrashEntry<>(entryC2, entryC));
            verify(storage).collect(new GarbageCollector.TrashEntry<>(entryD2, entryD));
        }
    }

    @Test
    void collectWithSnapshot2() {
        try (var s = gc.registerSnapshot(() -> SEQ_NUMBER + 2)) {
            registerGarbage();
            gc.collect(SEQ_NUMBER + 2, storage);

            verify(storage).collect(new GarbageCollector.TrashEntry<>(entryC2, entryC));
            verify(storage).collect(new GarbageCollector.TrashEntry<>(entryD2, entryD));
            verify(storage).collect(new GarbageCollector.TrashEntry<>(null, entryD2));
        }
    }
    @Test
    void collectWithSnapshot3() {
        try (var s = gc.registerSnapshot(() -> SEQ_NUMBER)) {
            registerGarbage();
            gc.collect(SEQ_NUMBER + 2, storage);
        }
    }

    @Test
    void collectWithSnapshot4() {
        try (var s = gc.registerSnapshot(() -> SEQ_NUMBER)) {
            try (var s2 = gc.registerSnapshot(() -> SEQ_NUMBER + 1)) {
                registerGarbage();
                gc.collect(SEQ_NUMBER + 2, storage);
            }
        }
    }

    @Test
    void collectWithSnapshot5() {
        try (var s = gc.registerSnapshot(() -> SEQ_NUMBER + 1)) {
            try (var s2 = gc.registerSnapshot(() -> SEQ_NUMBER + 2)) {
                registerGarbage();
            }

            gc.collect(SEQ_NUMBER + 2, storage);

            verify(storage).collect(new GarbageCollector.TrashEntry<>(entryC2, entryC));
            verify(storage).collect(new GarbageCollector.TrashEntry<>(entryD2, entryD));
        }
    }
}