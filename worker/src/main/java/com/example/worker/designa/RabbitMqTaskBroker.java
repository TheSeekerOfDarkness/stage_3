package com.example.worker.designa;

import com.example.common.model.CrawlTask;
import com.example.common.model.IndexTask;
import com.example.worker.WorkerConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * RabbitMQ-backed TaskBroker with durable queues, manual acks, bounded retries, and DLQs.
 */
public class RabbitMqTaskBroker implements TaskBroker {
    public static final String CRAWL_QUEUE = "crawl.tasks";
    public static final String INDEX_QUEUE = "index.tasks";
    public static final String CRAWL_DLQ = "crawl.dlq";
    public static final String INDEX_DLQ = "index.dlq";
    public static final String INDEX_PUBLISHED_EXCHANGE = "index.published";
    private static final int MAX_ATTEMPTS = 3;

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final WorkerConfig config;
    private Connection connection;
    private final Object publishLock = new Object();
    private Channel publishChannel;

    public RabbitMqTaskBroker(WorkerConfig config) {
        this.config = config;
    }

    @Override
    public synchronized void start() throws Exception {
        if (connection != null && connection.isOpen()) {
            return;
        }
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(config.rabbitmqHost);
        factory.setPort(config.rabbitmqPort);
        factory.setUsername(config.rabbitmqUser);
        factory.setPassword(config.rabbitmqPass);
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(2000);
        this.connection = factory.newConnection("worker-" + config.workerId);
        this.publishChannel = connection.createChannel();
        declareTopology(this.publishChannel);
    }

    private static void declareTopology(Channel ch) throws IOException {
        ch.queueDeclare(CRAWL_QUEUE, true, false, false, null);
        ch.queueDeclare(INDEX_QUEUE, true, false, false, null);
        ch.queueDeclare(CRAWL_DLQ, true, false, false, null);
        ch.queueDeclare(INDEX_DLQ, true, false, false, null);
        ch.exchangeDeclare(INDEX_PUBLISHED_EXCHANGE, "fanout", true);
    }

    @Override
    public void publishCrawlTask(CrawlTask task) throws Exception {
        publishPersistent(CRAWL_QUEUE, GSON.toJson(task));
    }

    @Override
    public void publishIndexTask(IndexTask task) throws Exception {
        publishPersistent(INDEX_QUEUE, GSON.toJson(task));
    }

    @Override
    public void publishSnapshotNotification(SnapshotManifest manifest) throws Exception {
        byte[] body = GSON.toJson(manifest).getBytes(StandardCharsets.UTF_8);
        synchronized (publishLock) {
            ensurePublishChannel();
            publishChannel.basicPublish(INDEX_PUBLISHED_EXCHANGE, "", null, body);
        }
    }

    private void publishPersistent(String queue, String json) throws Exception {
        byte[] body = json.getBytes(StandardCharsets.UTF_8);
        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
            .deliveryMode(2)
            .contentType("application/json")
            .build();
        synchronized (publishLock) {
            ensurePublishChannel();
            publishChannel.basicPublish("", queue, props, body);
        }
    }

    private void ensurePublishChannel() throws IOException {
        if (publishChannel == null || !publishChannel.isOpen()) {
            publishChannel = connection.createChannel();
            declareTopology(publishChannel);
        }
    }

    @Override
    public AutoCloseable startCrawlConsumer(int concurrency, TaskHandler<CrawlTask> handler) throws Exception {
        return startQueueConsumers(CRAWL_QUEUE, CRAWL_DLQ, concurrency, body -> {
            CrawlTask task = GSON.fromJson(body, CrawlTask.class);
            try {
                handler.handle(task);
                return null;
            } catch (Exception e) {
                if (task.attempt < MAX_ATTEMPTS) {
                    task.attempt += 1;
                    return new RetryOrDlq(CRAWL_QUEUE, GSON.toJson(task), e);
                }
                return new RetryOrDlq(CRAWL_DLQ, GSON.toJson(task), e);
            }
        });
    }

    @Override
    public AutoCloseable startIndexConsumer(int concurrency, TaskHandler<IndexTask> handler) throws Exception {
        return startQueueConsumers(INDEX_QUEUE, INDEX_DLQ, concurrency, body -> {
            IndexTask task = GSON.fromJson(body, IndexTask.class);
            try {
                handler.handle(task);
                return null;
            } catch (Exception e) {
                if (task.attempt < MAX_ATTEMPTS) {
                    task.attempt += 1;
                    return new RetryOrDlq(INDEX_QUEUE, GSON.toJson(task), e);
                }
                return new RetryOrDlq(INDEX_DLQ, GSON.toJson(task), e);
            }
        });
    }

    @FunctionalInterface
    private interface MessageProcessor {
        RetryOrDlq process(String jsonBody);
    }

    private static final class RetryOrDlq {
        final String targetQueue;
        final String payload;
        final Exception error;

        RetryOrDlq(String targetQueue, String payload, Exception error) {
            this.targetQueue = targetQueue;
            this.payload = payload;
            this.error = error;
        }
    }

    private AutoCloseable startQueueConsumers(String queue,
                                              String dlq,
                                              int concurrency,
                                              MessageProcessor processor) throws Exception {
        List<Channel> channels = new ArrayList<>();
        List<String> tags = new ArrayList<>();

        for (int i = 0; i < Math.max(1, concurrency); i++) {
            Channel channel = connection.createChannel();
            declareTopology(channel);
            channel.basicQos(1);

            DeliverCallback callback = (consumerTag, delivery) -> {
                String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
                long deliveryTag = delivery.getEnvelope().getDeliveryTag();
                try {
                    RetryOrDlq fallback = processor.process(json);
                    if (fallback != null) {
                        byte[] bytes = fallback.payload.getBytes(StandardCharsets.UTF_8);
                        AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                            .deliveryMode(2)
                            .contentType("application/json")
                            .build();
                        channel.basicPublish("", fallback.targetQueue, props, bytes);
                    }
                    channel.basicAck(deliveryTag, false);
                } catch (Exception e) {
                    try {
                        channel.basicPublish("", dlq, null, json.getBytes(StandardCharsets.UTF_8));
                        channel.basicAck(deliveryTag, false);
                    } catch (Exception ignored) {
                    }
                }
            };

            String tag = channel.basicConsume(queue, false, callback, consumerTag -> {});
            channels.add(channel);
            tags.add(tag);
        }

        return () -> {
            for (int i = 0; i < channels.size(); i++) {
                Channel ch = channels.get(i);
                String tag = tags.get(i);
                try {
                    if (ch.isOpen()) {
                        ch.basicCancel(tag);
                        ch.close();
                    }
                } catch (Exception ignored) {
                }
            }
        };
    }

    @Override
    public AutoCloseable subscribeSnapshotNotifications(Consumer<SnapshotManifest> listener) throws Exception {
        Channel ch = connection.createChannel();
        declareTopology(ch);
        String queueName = ch.queueDeclare("", false, true, true, null).getQueue();
        ch.queueBind(queueName, INDEX_PUBLISHED_EXCHANGE, "");

        DeliverCallback callback = (tag, delivery) -> {
            try {
                String json = new String(delivery.getBody(), StandardCharsets.UTF_8);
                SnapshotManifest manifest = GSON.fromJson(json, SnapshotManifest.class);
                if (manifest != null) {
                    listener.accept(manifest);
                }
            } catch (Exception ignored) {
            }
        };
        String tag = ch.basicConsume(queueName, true, callback, consumerTag -> {});

        return () -> {
            try {
                if (ch.isOpen()) {
                    ch.basicCancel(tag);
                    ch.close();
                }
            } catch (Exception ignored) {
            }
        };
    }

    @Override
    public synchronized void close() {
        try {
            if (publishChannel != null && publishChannel.isOpen()) {
                publishChannel.close();
            }
        } catch (Exception ignored) {
        }
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (Exception ignored) {
        }
    }
}
