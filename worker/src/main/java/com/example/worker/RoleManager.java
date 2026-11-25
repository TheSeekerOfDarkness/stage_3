package com.example.worker;

import com.example.common.obs.StructuredLogger;
import com.example.worker.backend.ClusterBackend;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

/**
 * Coordinates runtime role changes, drain state, backend loops, and Consul search registration.
 */
public class RoleManager {
    private final ClusterBackend backend;
    private final ConsulRegistrar consulRegistrar;
    private final StructuredLogger logger;

    private final EnumSet<WorkerRole> activeRoles;
    private volatile boolean draining = false;
    private volatile int boundPort = 0;

    public RoleManager(Set<WorkerRole> initialRoles,
                       ClusterBackend backend,
                       ConsulRegistrar consulRegistrar,
                       StructuredLogger logger) {
        this.activeRoles = initialRoles == null || initialRoles.isEmpty()
            ? EnumSet.noneOf(WorkerRole.class)
            : EnumSet.copyOf(initialRoles);
        this.backend = backend;
        this.consulRegistrar = consulRegistrar;
        this.logger = logger;
    }

    public synchronized void initialize(int port) {
        this.boundPort = port;
        backend.onRolesChanged(getActiveRoles());
        if (activeRoles.contains(WorkerRole.SEARCH) && !draining && port > 0) {
            consulRegistrar.register(port);
        }
    }

    public synchronized Set<WorkerRole> getActiveRoles() {
        return Collections.unmodifiableSet(EnumSet.copyOf(activeRoles));
    }

    public boolean hasRole(WorkerRole role) {
        synchronized (this) {
            return activeRoles.contains(role);
        }
    }

    public boolean isDraining() {
        return draining;
    }

    public boolean isConsulRegistered() {
        return consulRegistrar != null && consulRegistrar.isRegistered();
    }

    public synchronized void setRoles(Set<WorkerRole> newRoles) {
        EnumSet<WorkerRole> previous = EnumSet.copyOf(activeRoles);
        activeRoles.clear();
        if (newRoles != null) {
            activeRoles.addAll(newRoles);
        }

        backend.onRolesChanged(getActiveRoles());

        boolean hadSearch = previous.contains(WorkerRole.SEARCH);
        boolean hasSearch = activeRoles.contains(WorkerRole.SEARCH);
        if (!hadSearch && hasSearch && !draining && boundPort > 0) {
            consulRegistrar.register(boundPort);
        } else if (hadSearch && !hasSearch && consulRegistrar.isRegistered()) {
            consulRegistrar.deregister();
        }

        if (logger != null) {
            logger.log("role_changed", Map.of(
                "previous_roles", WorkerRole.toWireNames(previous),
                "active_roles", WorkerRole.toWireNames(activeRoles)
            ));
        }
    }

    public synchronized void beginDrain() {
        if (!draining) {
            draining = true;
            if (logger != null) {
                logger.log("readiness_changed", Map.of(
                    "ready", false,
                    "reason", "draining"
                ));
            }
        }
        if (consulRegistrar != null && consulRegistrar.isRegistered()) {
            consulRegistrar.deregister();
        }
    }

    public synchronized void resume() {
        if (draining) {
            draining = false;
            if (logger != null) {
                logger.log("readiness_changed", Map.of(
                    "ready", activeRoles.contains(WorkerRole.SEARCH),
                    "reason", "resumed"
                ));
            }
        }
        if (activeRoles.contains(WorkerRole.SEARCH) && boundPort > 0 && consulRegistrar != null) {
            consulRegistrar.register(boundPort);
        }
    }
}
