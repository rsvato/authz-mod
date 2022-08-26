package net.quanzy.authzmod.db;

import net.quanzy.authzmod.db.utils.Utils;
import org.apache.commons.codec.binary.Hex;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;

public class AuthzRecord {
    private final ByteBuffer contents;

    public AuthzRecord(byte[] username, byte[] passwordHash) {
        contents = ByteBuffer.allocate(username.length + passwordHash.length + 2 * Integer.BYTES);
        contents.putInt(username.length);
        contents.put(username);
        contents.putInt(passwordHash.length);
        contents.put(passwordHash);
    }

    private AuthzRecord(ByteBuffer contents) {
        this.contents = contents.duplicate();
    }

    public static AuthzRecord create(String username, String password) {
        return new AuthzRecord(username.getBytes(StandardCharsets.UTF_8), Hex.encodeHexString(Utils.digest(password)).getBytes());
    }

    public static AuthzRecord read(FileChannel channel) throws IOException {
        AuthzRecord result = null;
        if (channel.isOpen() && (channel.size() - channel.position()) > Integer.BYTES) {
            ByteBuffer rsize = ByteBuffer.allocate(Integer.BYTES);
            channel.read(rsize);
            rsize.flip();
            int recordSize = rsize.getInt();
            if ((channel.size() - channel.position()) >= recordSize) {
                ByteBuffer recordBuffer = ByteBuffer.allocate(recordSize);
                channel.read(recordBuffer);
                result = new AuthzRecord(recordBuffer);
            }
        }
        return result;
    }

    /**
     * Returns buffer copy and rewinds it.
     * @return view of buffer
     */
    public ByteBuffer contents() {
        return contents.asReadOnlyBuffer().flip();
    }

    public String getUsername() {
        ByteBuffer view = contents();
        int userNameLength = view.getInt();
        byte[] temp = new byte[userNameLength];
        view.get(temp);
        return new String(temp, StandardCharsets.UTF_8);
    }

    public String getHash() {
        ByteBuffer view = contents();
        int userNameLength = view.getInt();
        view = view.position(4 + userNameLength);
        int hashLength = view.getInt();
        byte[] temp = new byte[hashLength];
        view.get(temp);
        return new String(temp);
    }

    int length() {
        return contents().remaining();
    }
}
