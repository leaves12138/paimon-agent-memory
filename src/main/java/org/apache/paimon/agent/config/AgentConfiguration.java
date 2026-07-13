package org.apache.paimon.agent.config;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** The two deliberately separate configuration domains used by the agent. */
public final class AgentConfiguration {

    private final Map<String, String> catalogOptions;
    private final ProjectConfig project;

    public AgentConfiguration(Map<String, String> catalogOptions, ProjectConfig project) {
        this.catalogOptions = Collections.unmodifiableMap(new LinkedHashMap<>(catalogOptions));
        this.project = Objects.requireNonNull(project, "project");
    }

    public Map<String, String> catalogOptions() {
        return catalogOptions;
    }

    public ProjectConfig project() {
        return project;
    }
}
