package net.quanzy.authzmod.db.operations

import org.junit.jupiter.api.Test

class IndexOperationsTest {
    val indexOperations = IndexOperations(String::class.java)

    val testIndices = mapOf(
        "key1" to 100L,
        "key2" to 200L,
        "key3" to 300L
    )

    @Test
    fun testReadAndWriteIndex() {
        val indexFile = indexOperations.writeIndex(testIndices)
        assert(indexFile.exists())
        assert(indexFile.length() > 0)
        indexOperations.readIndex(indexFile).also { readIndices ->
            assert(readIndices.size == testIndices.size)
            testIndices.forEach { (key, offset) ->
                assert(readIndices[key] == offset)
            }
        }
    }

}