package net.quanzy.authzmod.db;

import net.quanzy.authzmod.db.operations.DataOperations;
import net.quanzy.authzmod.db.operations.FlushResult;
import net.quanzy.authzmod.db.operations.IndexOperations;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple file-based table storing records with a key.
 *
 * @param <KEY>    type of the key
 * @param <RECORD> type of the record
 */
public class Table<KEY, RECORD extends AbstractRecord<KEY>> {
    private final File dataFile;
    private final File indexFile;
    private final ConcurrentHashMap<KEY, RECORD> newRecords = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<KEY, RECORD> cache = new ConcurrentHashMap<>();
    private final Class<RECORD> klazz;
    private final ConcurrentHashMap<KEY, Long> keyOffsets = new ConcurrentHashMap<>();
    private final IndexOperations<KEY> indexOperations;
    private final DataOperations<RECORD, KEY> dataOperations;
    private boolean readOnly = false;
    private static final Logger logger = LoggerFactory.getLogger(Table.class);

    /**
     * Private constructor to enforce the use of the factory method.
     */
    private Table(File dataFile, Class<RECORD> klazz, Class<KEY> keyKlazz) {
        this.dataFile = dataFile;
        this.klazz = klazz;
        this.indexFile = new File(dataFile.getAbsolutePath() + ".idx");
        this.indexOperations = new IndexOperations<>(keyKlazz);
        this.dataOperations = new DataOperations<>();
    }

    boolean fileExists() {
        return dataFile.exists() && dataFile.canWrite();
    }

    boolean indexExists() {
        return indexFile.exists() && indexFile.canRead();
    }

    void delete() {
        if (!dataFile.delete()) {
            throw new RuntimeException("Cannot delete db file " + dataFile.getAbsolutePath());
        }
        if (indexExists()) {
            if (!indexFile.delete()) {
                throw new RuntimeException("Cannot delete index file " + indexFile.getAbsolutePath());
            }
        }
    }

    long size() {
        return dataFile.length();
    }

    long records() {
        return newRecords.size();
    }

    void readRecord() throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                RECORD record;
                do {
                    record = readRecord(channel);
                    if (record != null) {
                        newRecords.put(record.getKey(), record);
                    }
                } while (record != null);
            }
        }
    }

    @SuppressWarnings("unchecked")
    RECORD readRecord(FileChannel channel) throws IOException {
        RECORD result = null;
        if (channel.isOpen() && (channel.size() - channel.position()) > Integer.BYTES) {
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
        Map<KEY, Long> offsets = indexOperations.readIndex(indexFile);
        keyOffsets.clear();
        keyOffsets.putAll(offsets);
    }

    public Optional<RECORD> getRecordLazily(KEY key)  {
        RECORD result = cache.computeIfAbsent(key, k -> readRecordByOffset(key));
        if (result == null) {
            result = newRecords.get(key);
        }
        return Optional.ofNullable(result);
    }

    @Nullable
    private RECORD readRecordByOffset(KEY key) {
        Long offset = keyOffsets.get(key);
        if (offset == null) return null;
        try (RandomAccessFile raf = new RandomAccessFile(dataFile, "r")) {
            try (FileChannel channel = raf.getChannel()) {
                channel.position(offset);
                return readRecord(channel);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot read record for key " + key + " at offset " + offset, e);
        }
    }

    void addRecord(RECORD record) {
        if (readOnly) {
            throw new RuntimeException("Cannot add record to a read-only table");
        }
        newRecords.put(record.getKey(), record);
    }

    synchronized void flush() throws IOException {
        logger.debug("Flushing {} records to file {}", newRecords.size(), dataFile.getAbsolutePath());
        Map<KEY, Long> offsets = writeData(newRecords, dataFile);

        if (! offsets.isEmpty()) {
            writeIndex(offsets, indexFile);
        } else {
            logger.debug("No index to flush for DB {}", dataFile.getAbsolutePath());
        }
    }

    private Map<KEY, Long> writeData(Map<KEY, RECORD> records, File dataFile) throws IOException {
        FlushResult<KEY> result = dataOperations.writeData(records.values(), dataFile);
        File tempFile = result.getDataFile();

        if (!tempFile.renameTo(dataFile)) {
            throw new RuntimeException("Cannot save table from " + tempFile.getAbsolutePath() +
                    " to " + dataFile.getAbsolutePath());
        }
        return result.getIndexOffsets();
    }

    private void writeIndex(Map<KEY, Long> offsets, File indexFile) throws IOException {
        File tempIndexFile = indexOperations.writeIndex(offsets);
        Files.move(tempIndexFile.toPath(), indexFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Factory method to create or read a table from a file.
     * If the file does not exist, it will be created.
     *
     * @param path  path to the file
     * @param klass class of the record
     * @param <K>   type of the key
     * @param <R>   type of the record
     * @return instance of Table
     */
    public static <K, R extends AbstractRecord<K>> Table<K, R> createOrRead(String path, Class<R> klass, Class<K> keyKlazz) {
        return createOrRead(new File(path), klass, keyKlazz);
    }

    /**
     * Factory method to create or read a table from a file.
     * If the file does not exist, it will be created.
     *
     * @param dbFile database file.
     * @param klass  class of the record
     * @param <K>    type of the key
     * @param <R>    type of the record
     * @return instance of Table
     */
    public static <K, R extends AbstractRecord<K>> Table<K, R> createOrRead(File dbFile, Class<R> klass, Class<K> keyKlazz) {
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
                    table.readRecord();
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
        return Optional.ofNullable(newRecords.get(key));
    }

    public boolean isReadOnly() {
        return readOnly;
    }

    public int recordCount() {
        return newRecords.isEmpty()? keyOffsets.size() : newRecords.size();
    }
}
