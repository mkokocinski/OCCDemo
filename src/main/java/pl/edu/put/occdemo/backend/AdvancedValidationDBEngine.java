package pl.edu.put.occdemo.backend;

import pl.edu.put.occdemo.DBEngine;

import java.util.concurrent.atomic.AtomicReference;

public class AdvancedValidationDBEngine<K extends Comparable<? super K>, V> extends AbstractDBEngine<K, V> {

    // The head of the commit queue. It points to the last committed node.
    private final AtomicReference<CommitQueueNode<K, V>> commitQueueHead = new AtomicReference<>(new CommitQueueNode<>(null));

    AdvancedValidationDBEngine(boolean mv) {
        super(mv);
    }

    public static <K extends Comparable<K>, V> DBEngine<K, V> getInstance(boolean mv) {
        return new AdvancedValidationDBEngine<>(mv);
    }

    @Override
    protected boolean doCommit(AbstractExecutor<K, V> executor) {
        if (executor.noUpdatesAndNoValidation())
            return true;

        var lastCommitted = commitQueueHead.get();

        if (!executor.validate())
            return false;

        // Traverse all the nodes which were not yet committed when the validation started.
        CommitQueueNode<K, V> own = validateAndInsertIntoCommitQueue(executor, lastCommitted);
        if (own == null) return false;

        // Wait till the last committed node points to us.
        while (commitQueueHead.get().next.get() != own)
            Thread.yield();

        // Commit and set us as the last committed.
        executor.applyUpdates(sequenceNumber++);
        commitQueueHead.set(own);

        return true;
    }

    private CommitQueueNode<K, V> validateAndInsertIntoCommitQueue(AbstractExecutor<K, V> executor, CommitQueueNode<K, V> from) {
        var own = new CommitQueueNode<>(executor);
        var curr = from;
        var next = from.next.get();
        while (true) {
            if (next != null) {
                if (!executor.validate(next.executor.getUpdates()))
                    return null;
                curr = next;
                next = next.next.get();
            } else {
                next = curr.next.compareAndExchange(null, own);
                if (next == own)
                    break; // The full validation has passed successfully.
            }
        }
        return own;
    }

    static class CommitQueueNode<K extends Comparable<? super K>, V> {
        final AbstractExecutor<K, V> executor;
        final AtomicReference<CommitQueueNode<K, V>> next;

        public CommitQueueNode(AbstractExecutor<K, V> executor) {
            this.executor = executor;
            this.next = new AtomicReference<>(null);
        }
    }
}
