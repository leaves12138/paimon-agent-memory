package org.apache.paimon.agent.dashboard;

import org.apache.paimon.agent.restore.RestoreType;

/** Restores one selected Dashboard session into the matching local client home. */
@FunctionalInterface
public interface DashboardSessionRestorer {

    DashboardRestoreResult restore(RestoreType type, String sessionId) throws Exception;
}
