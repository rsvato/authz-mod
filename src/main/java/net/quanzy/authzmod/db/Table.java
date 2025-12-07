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
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
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
        this.indexFile = new File(dataFile.getAbsolutePath() + ".idx");
        this.indexOperations = new IndexOperations<>(keyKlazz);
        this.dataOperations = new DataOperations<>(klazz);
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

    long idxSize() {
        return keyOffsets.size();
    }

    void readRecords(boolean fillIndex) throws IOException {
        dataOperations.readData(dataFile).iterator().forEachRemaining(
                result -> {
                    if (result.getRecord() != null) {
                        newRecords.put(result.getRecord().getKey(), result.getRecord());
                        if (fillIndex) {
                            keyOffsets.put(result.getRecord().getKey(), result.getOffset());
                        }
                    }
                }
        );
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
                return dataOperations.readRecord(channel);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot read record for key " + key + " at offset " + offset, e);
        }
    }

    void addRecord(RECORD record) {
        if (! newRecords.containsKey(record.getKey()) && ! keyOffsets.containsKey(record.getKey())) {
            newRecords.put(record.getKey(), record);
        }
    }

    synchronized void flush() throws IOException {
        logger.debug("Flushing {} records to file {}", newRecords.size(), dataFile.getAbsolutePath());
        Map<KEY, Long> offsets = writeData(newRecords, dataFile);

        if (offsets.isEmpty()) {
            logger.debug("No index to flush for DB {}", dataFile.getAbsolutePath());
        } else {
            writeIndex(offsets, indexFile);
        }
        newRecords.clear();
    }

    private Map<KEY, Long> writeData(Map<KEY, RECORD> records, File dataFile) throws IOException {
        FlushResult<KEY> result = dataOperations.writeData(records.values(), dataFile);
        File tempFile = result.getDataFile();
        mergeFiles(dataFile, tempFile);
        return result.getIndexOffsets();
    }

    private void writeIndex(Map<KEY, Long> offsets, File indexFile) throws IOException {
        File tempIndexFile = indexOperations.writeIndex(offsets);
        mergeFiles(indexFile, tempIndexFile);
    }

    private void mergeFiles(File mainFile, File update) throws IOException {
        try (FileChannel outputChannel = FileChannel.open(mainFile.toPath(), StandardOpenOption.APPEND)) {
            try (FileChannel tempChannel = FileChannel.open(update.toPath(), StandardOpenOption.READ)) {
                tempChannel.transferTo(0, tempChannel.size(), outputChannel);
            }
        }
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
                    File indexParent = table.indexFile.getParentFile();
                    if ((indexParent.exists() || indexParent.mkdirs()) && table.indexFile.createNewFile()) {
                        logger.warn("Index file {} does not exist. Created new empty index file.", table.indexFile);
                    }
                    table.readRecords(true);
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
