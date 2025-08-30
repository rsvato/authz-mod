package net.quanzy.authzmod.db;

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

public class Table<KEY, RECORD extends AbstractRecord<KEY>> {
    private final File file;
    private final ConcurrentHashMap<KEY, RECORD> records = new ConcurrentHashMap<>();
    private final Class<RECORD> klazz;

    private Table(File file, Class<RECORD> klazz) {
        this.file = file;
        this.klazz = klazz;
    }

    boolean fileExists() {
        return file.exists() && file.canWrite();
    }

    void delete() {
        file.delete();
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
    public RECORD read(FileChannel channel) throws IOException {
        RECORD result = null;
        if (
                channel.isOpen() &&
                        (channel.size() - channel.position()) > Integer.BYTES
        ) {
            ByteBuffer rsize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(rsize);
            rsize.flip();
            int recordSize = rsize.getInt();
            if ((channel.size() - channel.position()) >= recordSize) {
                ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
                channel.read(recordBuffer);
                result = (RECORD) RECORD.build(recordBuffer, klazz);
            }
        }
        return result;
    }

    void addRecord(RECORD record) {
        records.put(record.getKey(), record);
    }

    synchronized void flush() throws IOException {
        File tempFile = File.createTempFile("tmp", "db");
        SeekableByteChannel channel = Files.newByteChannel(Paths.get(tempFile.toURI()),
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        records.values().forEach(r -> write(channel, r));
        channel.close();
        tempFile.renameTo(file);
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

    public static<K, R extends AbstractRecord<K>> Table<K, R> createOrRead(String path, Class<R> klass) {
        File f = new File(path);
        if (f.exists()) {
            return new Table<>(f, klass);
        } else {
            try {
                File parent = f.getParentFile();
                if ((parent.exists() || parent.mkdirs()) && f.createNewFile()) {
                    return new Table<>(f, klass);
                } else {
                    throw new RuntimeException("Cannot create new db file " + path);
                }
            } catch (IOException e) {
                throw new RuntimeException("Cannot create new db file " + path, e);
            }
        }
    }

    public Optional<RECORD> getRecord(KEY key) {
        return Optional.ofNullable(records.get(key));
    }
}
