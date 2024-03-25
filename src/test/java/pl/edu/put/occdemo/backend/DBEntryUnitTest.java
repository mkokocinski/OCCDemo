package pl.edu.put.occdemo.backend;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DBEntryUnitTest {

    @Test
    void isCommitted() {
        var entry = new DBEntry<>("abc", "def");
        assertFalse(entry.isCommitted());

        entry.seqNumber = 0;
        assertTrue(entry.isCommitted());

        entry.seqNumber = 1;
        assertTrue(entry.isCommitted());

        entry.seqNumber = -1;
        assertFalse(entry.isCommitted());
    }

    @Test
    void equals() {
        var a = new DBEntry<>("a", "1", 10);
        var b = new DBEntry<>("a", "1", 10);
        var c = new DBEntry<>("a", "1", 0);
        var d = new DBEntry<>("a", "2", 10);
        var e = Integer.valueOf(5);

        assertEquals(a, a);
        assertEquals(a, b);
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertNotEquals(a, e);
        assertNotEquals(a, null);
    }
}