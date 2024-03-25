package pl.edu.put.occdemo.backend;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import pl.edu.put.occdemo.DBEngine;
import pl.edu.put.occdemo.DBInterface;
import pl.edu.put.occdemo.IsolationLevel;
import pl.edu.put.occdemo.backend.TestUtils.FakeTimer;

import java.util.List;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static pl.edu.put.occdemo.backend.TestUtils.entries;
import static pl.edu.put.occdemo.backend.TestUtils.runConcurrently;

class SerialValidationDBEngineSystemTest {

    DBEngine<String, String> engine;

    @BeforeEach
    void setUp() {
        engine = SerialValidationDBEngine.getInstance(false);
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void readCommitted0() {
        engine.execute((db) -> {
            assertEquals(null, db.read("a"));
            assertEquals(null, db.read("b"));
            assertEquals(null, db.read("c"));
            assertEquals(null, db.read("d"));
            assertEquals(null, db.read("e"));
            assertEquals(List.of(), db.rangeQuery("a", "f"));

            db.write("a", "1");
            db.write("c", "x");
            db.write("d", "4");
            db.write("e", "5");
            assertEquals("1", db.read("a"));
            assertEquals(null, db.read("b"));
            assertEquals("x", db.read("c"));
            assertEquals("4", db.read("d"));
            assertEquals("5", db.read("e"));
            assertEquals(entries("a", "1", "c", "x", "d", "4", "e", "5"), db.rangeQuery("a", "f"));

            db.write("c", "3");
            assertEquals("3", db.read("c"));
            assertEquals(entries("a", "1", "c", "3", "d", "4", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals("1", db.read("a"));
            assertEquals(null, db.read("b"));
            assertEquals("3", db.read("c"));
            assertEquals("4", db.read("d"));
            assertEquals("5", db.read("e"));
            assertEquals(entries("a", "1", "c", "3", "d", "4", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "3", "d", "4", "e", "5"), db.rangeQuery("a", "f"));

            db.write("c", "33");
            db.remove("d");
            assertEquals("33", db.read("c"));
            assertEquals(null, db.read("d"));
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));

            db.commit();

            db.write("c", "333");
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));

            db.write("c", "333");

            db.rollback();
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);

        int[] counter = new int[1];
        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));

            int cnt = counter[0]++;
            if (cnt == 0) {
                db.remove("a");
                db.write("c", "333");
                db.retry();
            } else {
                db.write("a", "11");
            }
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(entries("a", "11", "c", "33", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(List.of(), db.rangeQuery("a", "f", 0));
            assertEquals(entries("a", "11", "c", "33"), db.rangeQuery("a", "f", 2));
            assertEquals(entries("a", "11", "c", "33", "e", "5"), db.rangeQuery("a", "f", 5));
            assertEquals(entries("e", "5"), db.rangeQuery("a", "f", (x) -> x.getValue().length() == 1));
            assertEquals(entries("e", "5"), db.rangeQuery("a", "f", (x) -> x.getValue().length() == 1, 1));
        }, IsolationLevel.READ_COMMITTED);
    }

    @Test
    void readCommitted1() {
        engine.execute((db) -> {
            db.write("a", "1");
            db.write("c", "3");
            db.write("d", "4");
            db.write("e", "5");
        }, IsolationLevel.READ_COMMITTED);

        var timer = new FakeTimer();

        Consumer<DBInterface<String, String>> tx1 = (db) -> {
            db.write("c", "33");
            db.remove("d");
            timer.waitUntil(100); // now the second tx can continue
            timer.finish();
        };

        Consumer<DBInterface<String, String>> tx2 = (db) -> {
            timer.waitUntil(100);
            assertEquals("3", db.read("c"));
            assertEquals("4", db.read("d"));
            timer.finish();
        };

        runConcurrently(engine, tx1, IsolationLevel.READ_COMMITTED, tx2, IsolationLevel.READ_COMMITTED);

        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);
    }

    @Test
    void readCommitted2() {
        engine.execute((db) -> {
            db.write("a", "1");
            db.write("c", "3");
            db.write("d", "4");
            db.write("e", "5");
        }, IsolationLevel.READ_COMMITTED);

        var timer = new FakeTimer();

        Consumer<DBInterface<String, String>> tx1 = (db) -> {
            timer.waitUntil(50);
            db.write("c", "33");
            db.remove("d");
        };

        Consumer<DBInterface<String, String>> tx2 = (db) -> {
            assertEquals("3", db.read("c"));
            assertEquals("4", db.read("d"));
            timer.waitUntil(100);
            assertEquals("33", db.read("c"));
            assertEquals(null, db.read("d"));
        };

        runConcurrently(
                engine,
                tx1, IsolationLevel.READ_COMMITTED, () -> timer.finish(),
                tx2, IsolationLevel.READ_COMMITTED, () -> timer.finish());

        engine.execute((db) -> {
            assertEquals(entries("a", "1", "c", "33", "e", "5"), db.rangeQuery("a", "f"));
        }, IsolationLevel.READ_COMMITTED);
    }
}