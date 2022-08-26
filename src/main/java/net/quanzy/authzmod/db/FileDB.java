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

public class FileDB {
    private final File file;
    private final ConcurrentHashMap<String, AuthzRecord> records = new ConcurrentHashMap<>();

    private FileDB(File file) {
        this.file = file;
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
        FileChannel channel = new RandomAccessFile(file, "r").getChannel();
        AuthzRecord record = null;
        do {
            record = AuthzRecord.read(channel);
            if (record != null) {
                records.put(record.getUsername(), record);
                System.err.println(records.size());
            }
        } while (record != null);
        channel.close();
    }

    void addRecord(AuthzRecord record) {
        records.put(record.getUsername(), record);
    }

    synchronized void flush() throws IOException {
        File tempFile = File.createTempFile("tmp", "db");
        SeekableByteChannel channel = Files.newByteChannel(Paths.get(tempFile.toURI()),
                StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
        records.values().forEach(r -> write(channel, r));
        channel.close();
        tempFile.renameTo(file);
    }

    static void write(SeekableByteChannel channel, AuthzRecord record) {
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

    public void addRecord(String username, String password) {
        records.put(username, AuthzRecord.create(username, password));
    }


    public static FileDB createOrRead(String path) {
        File f = new File(path);
        if (f.exists()) {
            return new FileDB(f);
        } else {
            try {
                File parent = f.getParentFile();
                if ((parent.exists() || parent.mkdirs()) && f.createNewFile()) {
                    return new FileDB(f);
                } else {
                    throw new RuntimeException("Cannot create new db file " + path);
                }
            } catch (IOException e) {
                throw new RuntimeException("Cannot create new db file " + path, e);
            }
        }
    }

    public Optional<AuthzRecord> getRecord(String username) {
        return Optional.ofNullable(records.get(username));
    }
}
