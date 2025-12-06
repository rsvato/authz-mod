package net.quanzy.authzmod.db;

import net.quanzy.authzmod.db.utils.Utils;
import org.apache.commons.codec.binary.Hex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static org.junit.Assert.assertTrue;

public class TestTable {

    File dbFile;

    @Before
    public void setup() throws IOException {
        dbFile = Files.createTempFile("test-db", ".db").toFile();
    }

    @After
    public void teardown() {
        if (dbFile != null && dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    public void testFileDBCreatesFile() {
        Table<String, AuthzRecord> db = null;
        db = Table.createOrRead(dbFile.getAbsolutePath(), AuthzRecord.class, String.class);
        assertTrue(db.fileExists());
    }

    @Test
    public void testRecordCreateRead() {
        AuthzRecord record = AuthzRecord.create("foo", "bar");
        String hash = Hex.encodeHexString(Utils.digest("bar"));
        Assert.assertEquals("foo", record.getUsername());
        Assert.assertEquals(hash, record.getHash());
        Assert.assertEquals(75 /*3 + 64 + 8*/, record.length());
    }

    @Test
    public void testDnChangeAfterWrite() throws IOException {
        Table<String, AuthzRecord> db = null;
        db = Table.createOrRead(dbFile.getAbsolutePath(), AuthzRecord.class, String.class);
        assertTrue(db.fileExists());
        AuthzRecord record = AuthzRecord.create("foo", "bar");
        db.addRecord(record);
        db.flush();
        Assert.assertNotEquals(0L, db.size());
    }

    @Test
    public void testReadAfterFlush() throws IOException {
        Table<String, AuthzRecord> db = null;
        db = Table.createOrRead(dbFile.getAbsolutePath(), AuthzRecord.class, String.class);
        db.addRecord(AuthzRecord.create("andrew", "bar"));
        db.addRecord(AuthzRecord.create("bar", "baz"));
        db.addRecord(AuthzRecord.create("asd", "fgh"));
        db.flush();
        Assert.assertNotEquals(0L, db.size());

        Table<String, AuthzRecord> db1 = Table.createOrRead(dbFile.getAbsolutePath(), AuthzRecord.class, String.class);
        db1.read();
        Assert.assertEquals(3, db1.records());
        assertTrue(db1.getRecord("andrew").isPresent());
        assertTrue(db1.getRecord("bar").isPresent());
        assertTrue(db1.getRecord("asd").isPresent());
    }

    @Test
    public void testWithIndex() throws IOException {
        Table<String, AuthzRecord> db = Table.createOrRead(dbFile.getAbsolutePath(), AuthzRecord.class, String.class);
        db.addRecord(AuthzRecord.create("andrew", "bar"));
        db.addRecord(AuthzRecord.create("nicholas", "baz"));
        db.flush();
        Assert.assertNotEquals(0L, db.size());

        Table<String, AuthzRecord> db1 = Table.createOrRead(dbFile.getAbsolutePath(), AuthzRecord.class, String.class);
        db1.buildIndex();
        Optional<AuthzRecord> andrew = db1.getRecordLazily("andrew");
        assertTrue(andrew.isPresent());
        andrew.ifPresent(r -> {
            Assert.assertEquals("andrew", r.getUsername());
            Assert.assertEquals(Hex.encodeHexString(Utils.digest("bar")), r.getHash());
        });
    }

}
