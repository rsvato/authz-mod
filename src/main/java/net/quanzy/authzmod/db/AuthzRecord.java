package net.quanzy.authzmod.db;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import net.quanzy.authzmod.db.utils.Utils;
import org.apache.commons.codec.binary.Hex;

/**
 * Authorization record containing username and password hash.
 */
public class AuthzRecord extends AbstractRecord<String> {

    /**
     * Creates record from username and password hash.
     * @param username username in bytes
     * @param passwordHash password hash in bytes
     */
    private AuthzRecord(byte[] username, byte[] passwordHash) {
        init(Integer.BYTES + username.length + Integer.BYTES + passwordHash.length);
        put(username, passwordHash);
    }

    public AuthzRecord(ByteBuffer buffer) {
        super(buffer);
    }

    @Override
    public String getKey() {
        return getUsername();
    }

    /**
     * Creates record from username and password.
     * @param username user name
     * @param password password
     * @return created record
     */
    public static AuthzRecord create(String username, String password) {
        return new AuthzRecord(
            username.getBytes(StandardCharsets.UTF_8),
            Hex.encodeHexString(Utils.digest(password)).getBytes()
        );
    }

    /**
     * Extracts username from record.
     * @return user name
     */
    public String getUsername() {
        return new String(readString(contents()), StandardCharsets.UTF_8);
    }

    /**
     * Extracts password hash from record.
     * @return password hash
     */
    public String getHash() {
        return new String(readString(skipString()), StandardCharsets.UTF_8);
    }
}
