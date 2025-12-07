package net.quanzy.authzmod.db;

import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;

public abstract class AbstractRecord<KEY> {

    protected ByteBuffer contents;

    public AbstractRecord() {
        this.contents = ByteBuffer.allocate(0);
    }

    public AbstractRecord(ByteBuffer contents) {
        this.contents = contents.duplicate();
    }

    /**
     * Builds a record instance from the given ByteBuffer using reflection.
     *
     * @param recordBuffer the ByteBuffer containing the record data
     * @param clazz        the class of the record to be instantiated
     * @param <KEY>        the type of the key
     * @param <RECORD>     the type of the record
     * @return an instance of AbstractRecord<KEY>
     */
    @SuppressWarnings("unchecked")
    public static <KEY, RECORD> AbstractRecord<KEY> build(ByteBuffer recordBuffer, Class<? extends RECORD> clazz) {
        try {
            Constructor<? extends RECORD> declaredConstructor = clazz.getDeclaredConstructor(ByteBuffer.class);
            declaredConstructor.setAccessible(true);
            return (AbstractRecord<KEY>) declaredConstructor.newInstance(recordBuffer);
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Reads a length-prefixed byte array from the given ByteBuffer.
     * The first 4 bytes represent the length of the array, followed by the array data.
     *
     * @param view the ByteBuffer to read from
     * @return the byte array read from the buffer
     */
    protected static byte[] readString(ByteBuffer view) {
        int stringLength = view.getInt();
        byte[] temp = new byte[stringLength];
        view.get(temp);
        return temp;
    }
    /**
     * Returns the size of the buffer.
     * @return size of buffer
     */
    public int getSize() {
        return contents.capacity();
    }

    /**
     * Returns buffer copy and rewinds it.
     * @return view of buffer
     */
    public ByteBuffer contents() {
        ByteBuffer readOnlyBuffer = contents.asReadOnlyBuffer();
        if (contents.position() > 0) {
            return readOnlyBuffer.flip();
        }
        return readOnlyBuffer;
    }
    /**
     * Initializes the buffer with the given length.
     * @param length length of buffer
     */
    public void init(int length) {
        this.contents = ByteBuffer.allocate(length);
    }

    /**
     * Puts an integer value into the buffer.
     * @param value integer value
     */
    private void putInt(int value) {
        contents.putInt(value);
    }

    private void put(byte[] src) {
        putInt(src.length);
        contents.put(src);
    }
    /**
     * Puts multiple strings into the buffer.
     * Each string is prefixed with its length.
     * @param data first string
     * @param rest additional strings
     */
    public void put(String data, String... rest) {
        put(data.getBytes());
        for (String s : rest) {
            put(s.getBytes());
        }
    }

    /**
     * Puts multiple byte arrays into the buffer.
     * Each array is prefixed with its length.
     * @param data first byte array
     * @param rest additional byte arrays
     */
    public void put(byte[] data, byte[]... rest) {
        put(data);
        for (byte[] b : rest) {
            put(b);
        }
    }
    /**
     * Returns the length of the contents buffer.
     * @return length of contents
     */
    public int length() {
        return contents().remaining();
    }

    /**
     * Returns the key of the record.
     * @return key of record
     */
    public abstract KEY getKey();

    /**
     * Skips over a length-prefixed string in the buffer and returns the updated buffer position.
     * @return updated ByteBuffer position after skipping the string
     */
    protected ByteBuffer skipString() {
        ByteBuffer view = contents();
        int fieldLength = view.getInt();
        view = view.position(Integer.BYTES + fieldLength);
        return view;
    }

}