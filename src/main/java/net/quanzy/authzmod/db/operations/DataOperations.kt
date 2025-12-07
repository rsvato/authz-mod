package net.quanzy.authzmod.db.operations

import net.quanzy.authzmod.db.AbstractRecord
import org.slf4j.LoggerFactory
import java.io.File
import java.io.IOException
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

class DataOperations<RECORD : AbstractRecord<KEY>, KEY>(val recordClass: Class<RECORD>) {

    private val logger = LoggerFactory.getLogger(DataOperations::class.java)

    @Throws(IOException::class)
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
                currentOffset.getAndAdd(writeRecord(channel, r))
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

    @Throws(IOException::class)
    fun readData(dataFile: File, recordFactory: (ByteBuffer) -> RECORD): Sequence<RecordReadResult<RECORD>> = sequence {
        FileChannel.open(
            Paths.get(dataFile.toURI()),
            StandardOpenOption.READ
        ).use { channel ->
            while (channel.position() < channel.size()) {
                val offset = channel.position()
                val sizeBuffer = ByteBuffer.allocate(Int.SIZE_BYTES)
                channel.read(sizeBuffer)
                sizeBuffer.flip()
                val recordSize = sizeBuffer.int
                val recordBuffer = ByteBuffer.allocate(recordSize)
                channel.read(recordBuffer)
                recordBuffer.flip()
                val record = recordFactory(recordBuffer)
                yield(RecordReadResult(record, offset))
            }
        }
    }

    @Suppress("UNCHECKED_CAST")
    fun readRecord(buffer: ByteBuffer): RECORD {
        return AbstractRecord.build<KEY, RECORD>(buffer, recordClass) as RECORD
    }

    @Throws(IOException::class)
    fun readRecord(channel: FileChannel): RECORD? {
        var result: RECORD? = null
        if (channel.isOpen() && (channel.size() - channel.position()) > Integer.BYTES) {
            val rsize = ByteBuffer.allocate(Integer.BYTES)
            channel.read(rsize)
            rsize.flip()
            val recordSize = rsize.getInt()
            if ((channel.size() - channel.position()) >= recordSize) {
                val recordBuffer = ByteBuffer.allocate(recordSize)
                channel.read(recordBuffer)
                recordBuffer.flip()
                result = readRecord(recordBuffer)
            }
        }
        return result
    }
}

data class FlushResult<KEY>(
    val dataFile: File,
    val indexOffsets: Map<KEY, Long>
)

data class RecordReadResult<RECORD>(
    val record: RECORD,
    val offset: Long
)