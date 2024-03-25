package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.DBEngine;
import pl.edu.put.occdemo.DBInterface;
import pl.edu.put.occdemo.IsolationLevel;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

class TestUtils {
    public static List<Map.Entry<String, String>> entries(String... keysAndValues) {
        if (keysAndValues.length % 2 != 0) throw new IllegalArgumentException();

        var result = new ArrayList<Map.Entry<String, String>>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            result.add(new AbstractMap.SimpleImmutableEntry<>(keysAndValues[i], keysAndValues[i + 1]));
        }
        return result;
    }

    public static <K extends Comparable<? super K>, V> void runConcurrently(
            DBEngine<K, V> engine,
            Consumer<DBInterface<K, V>> tx1, IsolationLevel lvl1,
            Consumer<DBInterface<K, V>> tx2, IsolationLevel lvl2) {
        runConcurrently(engine, tx1, lvl1, () -> {
        }, tx2, lvl2, () -> {
        });
    }

    public static <K extends Comparable<? super K>, V> void runConcurrently(
            DBEngine<K, V> engine,
            Consumer<DBInterface<K, V>> tx1, IsolationLevel lvl1, Runnable callback1,
            Consumer<DBInterface<K, V>> tx2, IsolationLevel lvl2, Runnable callback2) {
        ExecutorService service = Executors.newFixedThreadPool(2);
        CountDownLatch latch = new CountDownLatch(2);
        service.execute(() -> {
            engine.execute(tx1, lvl1);
            callback1.run();
            latch.countDown();
        });
        service.execute(() -> {
            engine.execute(tx2, lvl2);
            callback2.run();
            latch.countDown();
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static class FakeTimer {
        final int numberOfThreads;
        final int[] times;
        private final AtomicInteger nextId = new AtomicInteger(0);
        // Thread local variable containing each thread's ID
        private final ThreadLocal<Integer> threadId =
                ThreadLocal.withInitial(() -> nextId.getAndIncrement());

        public FakeTimer() {
            this.numberOfThreads = 2;
            this.times = new int[numberOfThreads];
        }

        public FakeTimer(int numberOfThreads) {
            this.numberOfThreads = numberOfThreads;
            this.times = new int[numberOfThreads];
        }

        private boolean isIt(int time) {
            return Arrays.stream(times).allMatch((x) -> x >= time);
        }

        public synchronized void waitUntil(int time) {
            times[threadId.get()] = time;
            notifyAll();
            while (!isIt(time)) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public void finish() {
            waitUntil(Integer.MAX_VALUE);
        }
    }
}
