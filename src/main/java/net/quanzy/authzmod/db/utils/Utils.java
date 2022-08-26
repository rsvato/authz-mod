package net.quanzy.authzmod.db.utils;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Utils {

    private static MessageDigest digester;

    static {
        try {
            digester = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public static byte[] digest(String input) {
        return digester.digest(input.getBytes(StandardCharsets.UTF_8));
    }

}
