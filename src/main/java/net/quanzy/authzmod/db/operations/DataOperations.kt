package net.quanzy.authzmod.db.operations

import net.quanzy.authzmod.db.AbstractRecord
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

class DataOperations<RECORD: AbstractRecord<KEY>, KEY>(val recordClass: Class<RECORD>, val keyClass: Class<KEY>) {

    private val logger = LoggerFactory.getLogger(DataOperations::class.java)

    fun writeData(records: Collection<RECORD>, dataFile: File): FlushResult<KEY> {
        val offsets: MutableMap<KEY, Long> = HashMap()
        val tempFile = File.createTempFile("tmpdb-", ".db")
        val currentOffset = AtomicLong(dataFile.length())
        Files.newByteChannel(
            Paths.get(tempFile.toURI()),
            StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE
        ).use { channel ->
            records.forEach(Consumer { r: RECORD ->
                offsets[r.getKey()] = currentOffset.get()
                logger.trace("Setting offset for key {} to {}", r.getKey(), offsets.get(r.getKey()))
                currentOffset.set(writeRecord(channel, r))
            })
        }
        return FlushResult(tempFile, offsets)
    }

    fun writeRecord(channel: SeekableByteChannel, record: RECORD): Long {
        try {
            val length = record.length()
            val recordSize = ByteBuffer.allocate(Int.SIZE_BYTES + length)
            recordSize.putInt(length)
            recordSize.put(record.contents())
            channel.write(recordSize.flip())
            return channel.position()
        } catch (e: IOException) {
            throw RuntimeException(e)
        }
    }

}

data class FlushResult<KEY>(
    val dataFile: File,
    val indexOffsets: Map<KEY, Long>
)