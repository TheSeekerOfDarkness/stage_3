package com.example.worker.designb;

import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Callable executed across Hazelcast cluster members via IExecutorService to prove
 * distributed task execution placement.
 */
public class DistributedDemoCallable implements Callable<Map<String, Object>>, HazelcastInstanceAware, Serializable {
    private static final long serialVersionUID = 1L;

    private final String payload;
    private transient HazelcastInstance hazelcast;

    public DistributedDemoCallable(String payload) {
        this.payload = payload;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hazelcast = hazelcastInstance;
    }

    @Override
    public Map<String, Object> call() {
        Member local = hazelcast.getCluster().getLocalMember();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("member_uuid", local.getUuid().toString());
        result.put("address", local.getAddress().toString());
        result.put("worker_id", local.getAttribute("worker_id"));
        result.put("host_name", local.getAttribute("host_name"));
        result.put("payload", payload != null ? payload : "");
        result.put("executed_at_ms", System.currentTimeMillis());
        return result;
    }
}
