package com.example.worker.designa;

import java.nio.file.Path;
import java.util.List;

/**
 * Object storage abstraction for documents, index deltas, and versioned SQLite snapshots.
 */
public interface ObjectStorage {
    void ensureBucket(String bucket) throws Exception;

    boolean exists(String bucket, String key) throws Exception;

    void putBytes(String bucket, String key, byte[] data, String contentType) throws Exception;

    void putString(String bucket, String key, String content, String contentType) throws Exception;

    void putFile(String bucket, String key, Path sourceFile, String contentType) throws Exception;

    byte[] getBytes(String bucket, String key) throws Exception;

    String getString(String bucket, String key) throws Exception;

    void downloadToFile(String bucket, String key, Path destinationFile) throws Exception;

    List<String> listKeys(String bucket, String prefix) throws Exception;
}
