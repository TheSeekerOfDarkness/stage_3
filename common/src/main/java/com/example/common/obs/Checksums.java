package com.example.common.obs;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * SHA-256 helpers for document integrity checks and snapshot verification.
 */
public final class Checksums {
    private static final int BUFFER_SIZE = 8192;

    private Checksums() {
    }

    public static String sha256Hex(String content) {
        if (content == null) {
            throw new IllegalArgumentException("Content cannot be null");
        }
        return sha256Hex(content.getBytes(StandardCharsets.UTF_8));
    }

    public static String sha256Hex(byte[] bytes) {
        if (bytes == null) {
            throw new IllegalArgumentException("Bytes cannot be null");
        }
        MessageDigest digest = newSha256();
        digest.update(bytes);
        return toHex(digest.digest());
    }

    public static String sha256Hex(Path file) throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("File path cannot be null");
        }
        MessageDigest digest = newSha256();
        byte[] buffer = new byte[BUFFER_SIZE];
        try (InputStream in = Files.newInputStream(file)) {
            int read;
            while ((read = in.read(buffer)) != -1) {
                digest.update(buffer, 0, read);
            }
        }
        return toHex(digest.digest());
    }

    public static String documentSha256(String header, String body) {
        String safeHeader = header != null ? header : "";
        String safeBody = body != null ? body : "";
        MessageDigest digest = newSha256();
        digest.update(safeHeader.getBytes(StandardCharsets.UTF_8));
        digest.update((byte) '\n');
        digest.update("---\n".getBytes(StandardCharsets.UTF_8));
        digest.update(safeBody.getBytes(StandardCharsets.UTF_8));
        return toHex(digest.digest());
    }

    private static MessageDigest newSha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    private static String toHex(byte[] hash) {
        StringBuilder sb = new StringBuilder(hash.length * 2);
        for (byte b : hash) {
            sb.append(Character.forDigit((b >>> 4) & 0xF, 16));
            sb.append(Character.forDigit(b & 0xF, 16));
        }
        return sb.toString();
    }
}
