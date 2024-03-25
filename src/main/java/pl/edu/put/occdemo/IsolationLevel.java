package pl.edu.put.occdemo;

public enum IsolationLevel {
    READ_COMMITTED,
    REPEATABLE_READ,
    SNAPSHOT_ISOLATION,
    SERIALIZABLE,
    // a form of serializable isolation, which is easier to implement, and in which RO txns do not require final validation
    SERIALIZABLE_SNAPSHOT_ISOLATION
}
