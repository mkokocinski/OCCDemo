package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.DBEngine;

public class SerialValidationDBEngine<K extends Comparable<? super K>, V> extends AbstractDBEngine<K, V> {

    private final Object validationLock = new Object();

    SerialValidationDBEngine(boolean mv) {
        super(mv);
    }

    public static <K extends Comparable<K>, V> DBEngine<K, V> getInstance(boolean mv) {
        return new SerialValidationDBEngine<>(mv);
    }

    @Override
    protected boolean doCommit(AbstractExecutor<K, V> executor) {
        if (executor.noUpdatesAndNoValidation())
           return true;

        synchronized (validationLock) {
            if (!executor.validate())
                return false;
            executor.applyUpdates(sequenceNumber++);
        }
        return true;
    }
}
