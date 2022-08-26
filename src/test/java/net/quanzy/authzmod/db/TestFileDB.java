package net.quanzy.authzmod.db;

import net.quanzy.authzmod.db.utils.Utils;
import org.apache.commons.codec.binary.Hex;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class TestFileDB {

    @Test
    public void testFileDBCreatesFile() {
        FileDB db = null;
        try {
            db = FileDB.createOrRead("/tmp/test.db");
            Assert.assertTrue(db.fileExists());
        } finally {
            if (db != null) {
                db.delete();
            }
        }
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
        FileDB db = null;
        try {
            db = FileDB.createOrRead("/tmp/test.db");
            Assert.assertTrue(db.fileExists());
            AuthzRecord record = AuthzRecord.create("foo", "bar");
            db.addRecord(record);
            db.flush();
            Assert.assertNotEquals(0L, db.size());
        } finally {
            if (db != null) {
                db.delete();
            }
        }
    }

    @Test
    public void testReadAfterFlush() throws IOException {
        FileDB db = null;
        try {
            db = FileDB.createOrRead("/tmp/test.db");
            db.addRecord(AuthzRecord.create("foo", "bar"));
            db.addRecord(AuthzRecord.create("bar", "baz"));
            db.addRecord(AuthzRecord.create("asd", "fgh"));
            db.flush();
            Assert.assertNotEquals(0L, db.size());

            FileDB db1 = FileDB.createOrRead("/tmp/test.db");
            db1.read();
            Assert.assertEquals(3, db1.records());
            Assert.assertTrue(db1.getRecord("foo").isPresent());
            Assert.assertTrue(db1.getRecord("bar").isPresent());
            Assert.assertTrue(db1.getRecord("asd").isPresent());
        } finally {
            if (db != null) {
                db.delete();
            }
        }
    }

}
