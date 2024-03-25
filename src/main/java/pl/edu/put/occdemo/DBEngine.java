package pl.edu.put.occdemo;

import java.util.function.Consumer;

public interface DBEngine<K extends Comparable<? super K>, V> {

    void execute(Consumer<DBInterface<K, V>> transaction, IsolationLevel isolationLevel);
}
