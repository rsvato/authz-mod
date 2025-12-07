package net.quanzy.authzmod.db

import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.io.File
import java.io.IOException
import java.nio.file.Files

class TestLoad {

    lateinit var dbFile: File

    @BeforeEach
    @Throws(IOException::class)
    fun setup() {
        dbFile = Files.createTempFile("test-db", ".db").toFile()
    }

    @AfterEach
    @Throws(IOException::class)
    fun teardown() {
        if (Files.exists(dbFile.toPath())) {
            //Files.delete(dbFile.toPath())
        }
    }

    @Test
    fun `Massive load`() {
        val source = Table.createOrRead(dbFile, AuthzRecord::class.java, String::class.java)
        for (i in 0 until 1_000_000) {
            source.addRecord(AuthzRecord.create("user-$i", "password-$i"))
        }
        source.flush()
        assertTrue(source.size() > 0)

        val result = Table.createOrRead(dbFile, AuthzRecord::class.java, String::class.java)
        assertEquals(1_000_000, result.recordCount())
    }
}