package com.example.worker.designa;

import io.minio.BucketExistsArgs;
import io.minio.GetObjectArgs;
import io.minio.ListObjectsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import io.minio.Result;
import io.minio.StatObjectArgs;
import io.minio.errors.ErrorResponseException;
import io.minio.messages.Item;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * MinIO-backed ObjectStorage implementation for Design A.
 */
public class MinioObjectStorage implements ObjectStorage {
    private final MinioClient client;

    public MinioObjectStorage(String endpoint, String accessKey, String secretKey) {
        this.client = MinioClient.builder()
            .endpoint(endpoint)
            .credentials(accessKey, secretKey)
            .build();
    }

    @Override
    public void ensureBucket(String bucket) throws Exception {
        boolean exists = client.bucketExists(BucketExistsArgs.builder().bucket(bucket).build());
        if (!exists) {
            try {
                client.makeBucket(MakeBucketArgs.builder().bucket(bucket).build());
            } catch (ErrorResponseException e) {
                String code = e.errorResponse() != null ? e.errorResponse().code() : "";
                if (!"BucketAlreadyOwnedByYou".equals(code) && !"BucketAlreadyExists".equals(code)) {
                    throw e;
                }
            }
        }
    }

    @Override
    public boolean exists(String bucket, String key) throws Exception {
        try {
            client.statObject(StatObjectArgs.builder().bucket(bucket).object(key).build());
            return true;
        } catch (ErrorResponseException e) {
            if (isNotFound(e)) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public void putBytes(String bucket, String key, byte[] data, String contentType) throws Exception {
        try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
            client.putObject(
                PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .stream(in, data.length, -1)
                    .contentType(contentType != null ? contentType : "application/octet-stream")
                    .build()
            );
        }
    }

    @Override
    public void putString(String bucket, String key, String content, String contentType) throws Exception {
        putBytes(bucket, key, content.getBytes(StandardCharsets.UTF_8),
            contentType != null ? contentType : "text/plain; charset=utf-8");
    }

    @Override
    public void putFile(String bucket, String key, Path sourceFile, String contentType) throws Exception {
        long size = Files.size(sourceFile);
        try (InputStream in = Files.newInputStream(sourceFile)) {
            client.putObject(
                PutObjectArgs.builder()
                    .bucket(bucket)
                    .object(key)
                    .stream(in, size, -1)
                    .contentType(contentType != null ? contentType : "application/octet-stream")
                    .build()
            );
        }
    }

    @Override
    public byte[] getBytes(String bucket, String key) throws Exception {
        try (InputStream in = client.getObject(GetObjectArgs.builder().bucket(bucket).object(key).build())) {
            return in.readAllBytes();
        } catch (ErrorResponseException e) {
            if (isNotFound(e)) {
                throw new NoSuchElementException("Object not found: " + bucket + "/" + key);
            }
            throw e;
        }
    }

    @Override
    public String getString(String bucket, String key) throws Exception {
        return new String(getBytes(bucket, key), StandardCharsets.UTF_8);
    }

    @Override
    public void downloadToFile(String bucket, String key, Path destinationFile) throws Exception {
        Path parent = destinationFile.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        try (InputStream in = client.getObject(GetObjectArgs.builder().bucket(bucket).object(key).build())) {
            Files.copy(in, destinationFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (ErrorResponseException e) {
            if (isNotFound(e)) {
                throw new NoSuchElementException("Object not found: " + bucket + "/" + key);
            }
            throw e;
        }
    }

    @Override
    public List<String> listKeys(String bucket, String prefix) throws Exception {
        List<String> keys = new ArrayList<>();
        try {
            Iterable<Result<Item>> results = client.listObjects(
                ListObjectsArgs.builder().bucket(bucket).prefix(prefix != null ? prefix : "").recursive(true).build()
            );
            for (Result<Item> result : results) {
                Item item = result.get();
                if (!item.isDir()) {
                    keys.add(item.objectName());
                }
            }
        } catch (ErrorResponseException e) {
            if (isNotFound(e)) {
                return Collections.emptyList();
            }
            throw e;
        }
        Collections.sort(keys);
        return keys;
    }

    private static boolean isNotFound(ErrorResponseException e) {
        if (e.response() != null && e.response().code() == 404) {
            return true;
        }
        String code = e.errorResponse() != null ? e.errorResponse().code() : "";
        return "NoSuchKey".equals(code) || "NoSuchBucket".equals(code) || "NoSuchObject".equals(code);
    }
}
