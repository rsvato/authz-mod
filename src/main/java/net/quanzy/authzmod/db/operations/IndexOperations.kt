package net.quanzy.authzmod.db.operations

import net.quanzy.authzmod.db.Serializers
import net.quanzy.authzmod.db.findSerializerForClass
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.ByteChannel
import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

class IndexOperations<KEY>(clazz: Class<KEY>) {
    private val logger = LoggerFactory.getLogger(IndexOperations::class.java)
    private val serializer: Serializers<KEY> = findSerializerForClass(clazz)

    fun writeIndex(indices: Map<KEY, Long>): File {
        val tempIndexFile = File.createTempFile("tmpidx-", ".idx")
        FileChannel.open(tempIndexFile.toPath(),
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
        ).use { indexChannel ->
            indices.forEach { (key: KEY, offset: Long) ->
                try {
                    writeIndexRecord(key, offset, indexChannel)
                } catch (e: IOException) {
                    throw RuntimeException(e)
                }
            }
        }
        logger.debug("Wrote index file {} with length {} presumably containing {} entries", tempIndexFile.absolutePath, tempIndexFile.length(), indices.size)
        return tempIndexFile
    }

    @Throws(IOException::class)
    private fun writeIndexRecord(key: KEY, offset: Long, indexChannel: FileChannel) {
        val keyBuffer = serializer.serialize(key)
        val recordSize = Int.SIZE_BYTES + Long.SIZE_BYTES + keyBuffer.capacity()
        val buffer = ByteBuffer.allocate(recordSize)
        buffer.putInt(recordSize)
        buffer.putLong(offset)
        buffer.put(keyBuffer)
        val toWrite = buffer.flip()
        var totalWritten = 0
        while (buffer.hasRemaining()) {
            val written = indexChannel.write(toWrite)
            if (written <= 0) break
            totalWritten += written
        }
        logger.debug("Wrote {} bytes for index entry for key {} at offset {}, recordSize {}", totalWritten, key, offset, recordSize)
    }

    fun readIndex(file: File): Map<KEY, Long> {
        FileChannel.open(file.toPath(), StandardOpenOption.READ).use { channel ->
            val keyOffsets = mutableMapOf<KEY, Long>()
            while (channel.position() < channel.size()) {
                readIndexRecord(channel, keyOffsets)
            }
            return keyOffsets
        }
    }

    @Throws(IOException::class)
    private fun readIndexRecord(channel: ByteChannel, offsets: MutableMap<KEY, Long>): Int {
        val rsize = ByteBuffer.allocate(Int.SIZE_BYTES)
        channel.read(rsize)
        rsize.position(0)
        val recordSize = rsize.getInt()
        val offsetBuffer = ByteBuffer.allocate(Long.SIZE_BYTES)
        channel.read(offsetBuffer)
        offsetBuffer.flip()
        val offset = offsetBuffer.getLong()
        val dataSize = recordSize - (Int.SIZE_BYTES + Long.SIZE_BYTES) // subtract size of recordSize and offset
        val recordBuffer = ByteBuffer.allocate(dataSize)
        channel.read(recordBuffer)
        recordBuffer.flip()
        val key = serializer.read(recordBuffer)
        offsets[key] = offset
        return recordSize
    }

}