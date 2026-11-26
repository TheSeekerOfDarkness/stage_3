package com.example.worker.designb;

import com.example.worker.WorkerConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import java.util.List;

/**
 * Builds embedded Hazelcast 5.5.0 cluster configuration from WorkerConfig.
 */
public final class HazelcastConfigBuilder {
    private static final List<String> MAP_NAMES = List.of(
        "documents",
        "books",
        "inverted_index",
        "crawl_claims",
        "in_flight_tasks",
        "cluster_meta"
    );
    private static final List<String> QUEUE_NAMES = List.of(
        "crawl_queue",
        "index_queue"
    );

    private HazelcastConfigBuilder() {
    }

    public static Config build(WorkerConfig workerConfig) {
        Config config = new Config();
        config.setClusterName(workerConfig.hazelcastClusterName);
        config.setProperty("hazelcast.local.localAddress", workerConfig.hostIp);
        config.setProperty("hazelcast.phone.home.enabled", "false");
        config.setProperty("hazelcast.logging.type", "slf4j");

        config.getMemberAttributeConfig().setAttribute("worker_id", workerConfig.workerId);
        config.getMemberAttributeConfig().setAttribute("host_name", workerConfig.hostName);

        NetworkConfig network = config.getNetworkConfig();
        network.setPort(workerConfig.hazelcastPort).setPortAutoIncrement(true);
        if (workerConfig.hazelcastPublicAddress != null && !workerConfig.hazelcastPublicAddress.isBlank()) {
            network.setPublicAddress(workerConfig.hazelcastPublicAddress.trim());
        }

        if ("multicast".equalsIgnoreCase(workerConfig.hazelcastJoinMode)) {
            network.getJoin().getTcpIpConfig().setEnabled(false);
            network.getJoin().getMulticastConfig().setEnabled(true);
        } else {
            network.getJoin().getMulticastConfig().setEnabled(false);
            TcpIpConfig tcp = network.getJoin().getTcpIpConfig().setEnabled(true);
            if (workerConfig.hazelcastMembers != null && !workerConfig.hazelcastMembers.isBlank()) {
                for (String member : workerConfig.hazelcastMembers.split(",")) {
                    if (!member.isBlank()) {
                        tcp.addMember(member.trim());
                    }
                }
            }
        }

        int backups = Math.max(0, workerConfig.hazelcastBackupCount);
        for (String mapName : MAP_NAMES) {
            config.getMapConfig(mapName)
                .setBackupCount(backups)
                .setAsyncBackupCount(0);
        }
        for (String queueName : QUEUE_NAMES) {
            config.getQueueConfig(queueName)
                .setBackupCount(backups)
                .setAsyncBackupCount(0)
                .setMaxSize(0);
        }

        config.getExecutorConfig("search-exec")
            .setPoolSize(Math.max(2, workerConfig.indexThreads));

        return config;
    }
}
