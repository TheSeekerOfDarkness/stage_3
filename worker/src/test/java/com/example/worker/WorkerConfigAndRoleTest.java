package com.example.worker;

import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class WorkerConfigAndRoleTest {

    @Test
    void parsesWorkerRolesIncludingCrawlAliasAndRejectsUnknown() {
        assertEquals(WorkerRole.INGEST, WorkerRole.fromString("ingest"));
        assertEquals(WorkerRole.INGEST, WorkerRole.fromString("CRAWL"));
        assertEquals(WorkerRole.INDEX, WorkerRole.fromString("index"));
        assertEquals(WorkerRole.SEARCH, WorkerRole.fromString("Search"));

        Set<WorkerRole> parsed = WorkerRole.parseSet("crawl, search");
        assertEquals(Set.of(WorkerRole.INGEST, WorkerRole.SEARCH), parsed);
        assertTrue(WorkerRole.parseSet("  ").isEmpty());

        assertThrows(IllegalArgumentException.class, () -> WorkerRole.fromString("unknown-role"));
        assertThrows(IllegalArgumentException.class, () -> WorkerRole.fromString("   "));
    }

    @Test
    void parsesWorkerConfigWithDefaultsAndOverrides() {
        WorkerConfig defaults = WorkerConfig.fromMap(Map.of());
        assertEquals(7000, defaults.port);
        assertEquals("127.0.0.1", defaults.hostIp);
        assertEquals("design-a", defaults.designMode);
        assertEquals(Set.of(WorkerRole.INGEST, WorkerRole.INDEX, WorkerRole.SEARCH), defaults.initialRoles);
        assertTrue(defaults.consulEnabled);

        WorkerConfig custom = WorkerConfig.fromMap(Map.of(
            "WORKER_ID", "node2-w1",
            "HOST_NAME", "lab-pc-02",
            "HOST_IP", "192.168.10.22",
            "PORT", "7010",
            "DESIGN_MODE", "design-b",
            "WORKER_ROLES", "crawl,index",
            "CRAWL_THREADS", "4",
            "INDEX_THREADS", "3",
            "CONSUL_ENABLED", "false",
            "HAZELCAST_BACKUP_COUNT", "2"
        ));

        assertEquals("node2-w1", custom.workerId);
        assertEquals("lab-pc-02", custom.hostName);
        assertEquals("192.168.10.22", custom.hostIp);
        assertEquals(7010, custom.port);
        assertEquals("design-b", custom.designMode);
        assertEquals(Set.of(WorkerRole.INGEST, WorkerRole.INDEX), custom.initialRoles);
        assertEquals(4, custom.crawlThreads);
        assertEquals(3, custom.indexThreads);
        assertFalse(custom.consulEnabled);
        assertEquals(2, custom.hazelcastBackupCount);
    }
}
