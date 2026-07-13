package org.apache.paimon.agent.dashboard;

/** Whether a dashboard row only exists in the collector buffer or is visible in Paimon. */
public enum DashboardStorageStatus {
    PENDING("pending"),
    UPLOADED("uploaded");

    private final String apiValue;

    DashboardStorageStatus(String apiValue) {
        this.apiValue = apiValue;
    }

    public String apiValue() {
        return apiValue;
    }
}
