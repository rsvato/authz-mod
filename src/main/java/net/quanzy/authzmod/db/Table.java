package net.quanzy.authzmod.db;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple file-based table storing records with a key.
 * @param <KEY> type of the key
 * @param <RECORD> type of the record
 */
public class Table<KEY, RECORD extends AbstractRecord<KEY>> {
    private final File file;
    private final File indexFile;
    private final ConcurrentHashMap<KEY, RECORD> records = new ConcurrentHashMap<>();
    private final Class<RECORD> klazz;
    private final Class<KEY> keyKlazz;
    private final ConcurrentHashMap<KEY, Long> keyOffsets = new ConcurrentHashMap<>();
    private boolean readOnly = false;
    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    /** Private constructor to enforce the use of the factory method. */
    private Table(File file, Class<RECORD> klazz, Class<KEY> keyKlazz) {
        this.file = file;
        this.klazz = klazz;
        this.keyKlazz = keyKlazz;
        this.indexFile = new File(file.getAbsolutePath() + ".idx");
    }

    boolean fileExists() {
        return file.exists() && file.canWrite();
    }

    boolean indexExists() {
        return indexFile.exists() && indexFile.canRead();
    }

    void delete() {
        if (! file.delete()) {
            throw new RuntimeException("Cannot delete db file " + file.getAbsolutePath());
        }
        if (indexExists()) {
            if (! indexFile.delete()) {
                throw new RuntimeException("Cannot delete index file " + indexFile.getAbsolutePath());
            }
        }
    }

    long size() {
        return file.length();
    }

    long records() {
        return records.size();
    }

    void read() throws IOException {
        try(RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try(FileChannel channel = raf.getChannel()) {
                RECORD record;
                do {
                    record = read(channel);
                    if (record != null) {
                        records.put(record.getKey(), record);
                    }
                } while (record != null);
            }
        }
    }

    @SuppressWarnings("unchecked")
    RECORD read(FileChannel channel) throws IOException {
        RECORD result = null;
        if (channel.isOpen() &&  (channel.size() - channel.position()) > Integer.BYTES) {
            ByteBuffer rsize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(rsize);
            rsize.flip();
            int recordSize = rsize.getInt();
            if ((channel.size() - channel.position()) >= recordSize) {
                ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
                channel.read(recordBuffer);
                recordBuffer.flip();
                result = (RECORD) RECORD.build(recordBuffer, klazz);
            }
        }
        return result;
    }

    void readIndex() throws IOException {
        try(RandomAccessFile raf = new RandomAccessFile(indexFile, "r")) {
            try(FileChannel channel = raf.getChannel()) {
                while(channel.position() < channel.size()) {
                    ByteBuffer rsize = ByteBuffer.allocate(Integer.BYTES);
                    channel.read(rsize);
                    rsize.flip();
                    int recordSize = rsize.getInt();
                    ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
                    channel.read(recordBuffer);
                    recordBuffer.flip();
                    KEY key = AbstractRecord.peekKey(recordBuffer, klazz, keyKlazz);
                    long offset = recordBuffer.getLong();
                    keyOffsets.put(key, offset);
                }
            }
        }
    }

    void buildIndex() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                while (channel.position() < channel.size()) {
                    long pos = channel.position();
                    ByteBuffer rsize = ByteBuffer.allocate(Integer.BYTES);
                    channel.read(rsize);
                    rsize.flip();
                    int recordSize = rsize.getInt();
                    ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
                    channel.read(recordBuffer);
                    recordBuffer.flip();
                    KEY key = AbstractRecord.peekKey(recordBuffer, klazz, keyKlazz);
                    keyOffsets.put(key, pos);
                    channel.position(pos + Integer.BYTES + recordSize);
                }
            }
        }
        readOnly = true;
    }

    public Optional<RECORD> getRecordLazily(KEY key) throws IOException {
        Long offset = keyOffsets.get(key);
        if (offset == null) return Optional.empty();
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                channel.position(offset);
                return Optional.ofNullable(read(channel));
            }
        }
    }

    void addRecord(RECORD record) {
        if (readOnly) {
            throw new RuntimeException("Cannot add record to a read-only table");
        }
        records.put(record.getKey(), record);
    }

    synchronized void flush() throws IOException {
        logger.info("Flushing {} records to file {}", records.size(), file.getAbsolutePath());
        File tempFile = File.createTempFile("tmp", "db");
        try (SeekableByteChannel channel = Files.newByteChannel(Paths.get(tempFile.toURI()),
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
            records.values().forEach(r -> {
                write(channel, r);
            });
        }

        if (!tempFile.renameTo(file)) {
            throw new RuntimeException("Cannot save table from " + tempFile.getAbsolutePath() +
                    " to " + file.getAbsolutePath());
        }

        if (!keyOffsets.isEmpty()) {
            File tempIndexFile = File.createTempFile("tmp", "idx");
            try (SeekableByteChannel indexChannel = Files.newByteChannel(Paths.get(tempIndexFile.toURI()),
                    StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE)) {
                keyOffsets.forEach((key, offset) -> {
                    try {
                        ByteBuffer keyBuffer = AbstractRecord.keyToBuffer(key);
                        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES + keyBuffer.remaining() + Long.BYTES);
                        buffer.putInt(keyBuffer.remaining());
                        buffer.put(keyBuffer);
                        buffer.putLong(offset);
                        indexChannel.write(buffer.flip());
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            if (!tempIndexFile.renameTo(indexFile)) {
                throw new RuntimeException("Cannot save index from " + tempIndexFile.getAbsolutePath() +
                        " to " + indexFile.getAbsolutePath());
            }
        } else {
            logger.debug("No index to flush for DB {}", file.getAbsolutePath());
        }
    }

    void write(SeekableByteChannel channel, RECORD record) {
        try {
            int length = record.length();
            ByteBuffer recordSize = ByteBuffer.allocate(Integer.BYTES + length);
            recordSize.putInt(length);
            recordSize.put(record.contents());
            channel.write(recordSize.flip());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Factory method to create or read a table from a file.
     * If the file does not exist, it will be created.
     * @param path path to the file
     * @param klass class of the record
     * @param <K> type of the key
     * @param <R> type of the record
     * @return instance of Table
     */
    public static<K, R extends AbstractRecord<K>> Table<K, R> createOrRead(String path, Class<R> klass, Class<K> keyKlazz) {
        return readFromFile(new File(path), klass, keyKlazz);
    }

    /**
     * Factory method to create or read a table from a file.
     * If the file does not exist, it will be created.
     * @param dbFile database file.
     * @param klass class of the record
     * @param <K> type of the key
     * @param <R> type of the record
     * @return instance of Table
     */
    public static <K, R extends AbstractRecord<K>> Table<K, R> readFromFile(File dbFile, Class<R> klass, Class<K> keyKlazz) {
        Table<K, R> table = new Table<>(dbFile, klass, keyKlazz);
        if (dbFile.exists()) {
            if (table.indexExists()) {
                try {
                    table.readIndex();
                    table.readOnly = true;
                } catch (IOException e) {
                    throw new RuntimeException("Cannot read index file " + table.indexFile.getAbsolutePath(), e);
                }
            } else {
                try {
                    table.read();
                } catch (IOException e) {
                    throw new RuntimeException("Cannot read db file " + dbFile.getAbsolutePath(), e);
                }
            }
            return table;
        } else {
            try {
                File parent = dbFile.getParentFile();
                if ((parent.exists() || parent.mkdirs()) && dbFile.createNewFile()) {
                    return table;
                } else {
                    throw new RuntimeException("Cannot create new db file " + dbFile.getAbsolutePath());
                }
            } catch (IOException e) {
                throw new RuntimeException("Cannot create new db file " + dbFile.getAbsolutePath(), e);
            }
        }
    }

    public Optional<RECORD> getRecord(KEY key) {
        return Optional.ofNullable(records.get(key));
    }

    public boolean isReadOnly() {
        return readOnly;
    }
}
