package com.example.worker.designa;

import com.example.common.model.CrawlTask;
import com.example.common.model.IndexTask;
import java.util.function.Consumer;

/**
 * Messaging abstraction for crawl/index work queues and snapshot broadcast notifications.
 */
public interface TaskBroker extends AutoCloseable {
    @FunctionalInterface
    interface TaskHandler<T> {
        void handle(T task) throws Exception;
    }

    void start() throws Exception;

    @Override
    void close();

    void publishCrawlTask(CrawlTask task) throws Exception;

    void publishIndexTask(IndexTask task) throws Exception;

    void publishSnapshotNotification(SnapshotManifest manifest) throws Exception;

    AutoCloseable startCrawlConsumer(int concurrency, TaskHandler<CrawlTask> handler) throws Exception;

    AutoCloseable startIndexConsumer(int concurrency, TaskHandler<IndexTask> handler) throws Exception;

    AutoCloseable subscribeSnapshotNotifications(Consumer<SnapshotManifest> listener) throws Exception;
}
