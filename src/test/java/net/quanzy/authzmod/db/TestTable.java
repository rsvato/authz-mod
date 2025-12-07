package net.quanzy.authzmod.db;

import net.quanzy.authzmod.db.utils.Utils;
import org.apache.commons.codec.binary.Hex;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;


public class TestTable {

    File dbFile;

    @BeforeEach
    public void setup() throws IOException {
        dbFile = Files.createTempFile("test-db", ".db").toFile();
    }

    @AfterEach
    public void teardown() throws IOException {
        if (dbFile != null && Files.exists(dbFile.toPath())) {
            Files.delete(dbFile.toPath());
        }
    }

    @Test
    public void testFileDBCreatesFile() {
        Table<String, AuthzRecord> db = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        assertTrue(db.fileExists());
    }

    @Test
    public void testRecordCreateRead() {
        AuthzRecord record = AuthzRecord.create("foo", "bar");
        String hash = Hex.encodeHexString(Utils.digest("bar"));
        assertEquals("foo", record.getUsername());
        assertEquals(hash, record.getHash());
        assertEquals(75 /*3 + 64 + 8*/, record.length());
    }

    @Test
    public void testDnChangeAfterWrite() throws IOException {
        Table<String, AuthzRecord> db = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        assertTrue(db.fileExists());
        AuthzRecord record = AuthzRecord.create("foo", "bar");
        db.addRecord(record);
        db.flush();
        assertNotEquals(0L, db.size());
    }

    @Test
    public void testReadAfterFlush() throws IOException {
        Table<String, AuthzRecord> db = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        db.addRecord(AuthzRecord.create("andrew", "bar"));
        db.addRecord(AuthzRecord.create("bar", "baz"));
        db.addRecord(AuthzRecord.create("asd", "fgh"));
        db.flush();
        assertNotEquals(0L, db.size());

        Table<String, AuthzRecord> db1 = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        db1.readRecords(false);
        assertEquals(3, db1.records());
        assertTrue(db1.getRecord("andrew").isPresent());
        assertTrue(db1.getRecord("bar").isPresent());
        assertTrue(db1.getRecord("asd").isPresent());
    }

    @Test
    public void testWithIndex() throws IOException {
        Table<String, AuthzRecord> db = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        db.addRecord(AuthzRecord.create("andrew", "bar"));
        db.addRecord(AuthzRecord.create("nicholas", "baz"));
        db.flush();
        assertNotEquals(0L, db.size());

        Table<String, AuthzRecord> db1 = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        Optional<AuthzRecord> nick = db1.getRecordLazily("nicholas");
        assertTrue(nick.isPresent(), "Record for 'nicholas' should be present");

        nick.ifPresent(r -> {
            assertEquals("nicholas", r.getUsername());
            assertEquals(Hex.encodeHexString(Utils.digest("baz")), r.getHash());
        });
        Optional<AuthzRecord> andrew = db1.getRecordLazily("andrew");

        assertTrue(andrew.isPresent(), "Record for 'andrew' should be present");
        andrew.ifPresent(r -> {
            assertEquals("andrew", r.getUsername());
            assertEquals(Hex.encodeHexString(Utils.digest("bar")), r.getHash());
        });
    }

    @Test
    public void testAppendDB() throws IOException {
        Table<String, AuthzRecord> db = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        db.addRecord(AuthzRecord.create("andrew", "bar"));
        db.addRecord(AuthzRecord.create("nicholas", "baz"));
        db.flush();
        assertNotEquals(0L, db.size());
        long oldSize = db.size();
        db.addRecord(AuthzRecord.create("michael", "quartz"));
        db.addRecord(AuthzRecord.create("dean", "ruby"));
        db.flush();
        assertNotEquals(oldSize, db.size());
        assertTrue(oldSize < db.size());

        Table<String, AuthzRecord> db1 = Table.createOrRead(dbFile, AuthzRecord.class, String.class);
        assertEquals(4, db1.idxSize());
        assertTrue(db1.getRecordLazily("michael").isPresent());
        assertTrue(db1.getRecordLazily("dean").isPresent());
        assertTrue(db1.getRecordLazily("andrew").isPresent());
        assertTrue(db1.getRecordLazily("nicholas").isPresent());

        db1.getRecord("andrew").ifPresent(r -> {
            assertEquals("andrew", r.getUsername());
            assertEquals(Hex.encodeHexString(Utils.digest("ruby")), r.getHash());
        });
    }

}
