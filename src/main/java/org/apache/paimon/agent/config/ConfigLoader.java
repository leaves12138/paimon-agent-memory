package org.apache.paimon.agent.config;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/** Loads paimon.properties and project.properties without mixing their namespaces. */
public final class ConfigLoader {

    public static final Path DEFAULT_PAIMON_CONFIG = Paths.get("paimon.properties");
    public static final Path DEFAULT_PROJECT_CONFIG = Paths.get("project.properties");

    private ConfigLoader() {}

    public static AgentConfiguration load(Path paimonPath, Path projectPath) {
        Properties paimon = loadRequired(paimonPath, "Paimon catalog");
        Properties project = loadRequired(projectPath, "project");

        Map<String, String> catalogOptions = new LinkedHashMap<>();
        paimon.stringPropertyNames().stream()
                .sorted()
                .forEach(name -> catalogOptions.put(name, paimon.getProperty(name)));
        if (catalogOptions.containsKey("type")) {
            throw new ConfigurationException(
                    "Unsupported Paimon catalog property: type; "
                            + "Paimon selects catalogs with metastore=<catalog-name>");
        }

        ProjectConfig projectConfig =
                new ProjectConfig(
                        required(project, "database"),
                        get(project, "sessions.table", "ai_chat_sessions"),
                        get(project, "messages.table", "ai_chat_messages"),
                        parseDuration(get(project, "collector.scan.interval", "5m")),
                        parseDuration(get(project, "collector.commit.interval", "5m")),
                        parseBoolean(project, "collector.run-once", false),
                        required(project, "collector.id"),
                        source(project, "codex", "~/.codex"),
                        source(project, "claude", "~/.claude"),
                        parseBoolean(project, "attachments.enabled", true),
                        parseBoolean(project, "attachments.download-remote", false),
                        parseBytes(get(project, "attachments.max-size", "100MB")),
                        parseInt(project, "collector.scan.max-records-per-source", 10_000),
                        parseInt(project, "collector.buffer.max-records", 10_000),
                        parseInt(project, "collector.retry.max-attempts", 10),
                        parseDuration(get(project, "collector.retry.initial-delay", "5s")),
                        new DashboardConfig(
                                parseBoolean(project, "dashboard.enabled", true),
                                get(project, "dashboard.host", "127.0.0.1"),
                                parseInt(project, "dashboard.port", 8787),
                                parseInt(project, "dashboard.page-size", 25),
                                parseInt(project, "dashboard.max-page-size", 100),
                                parseInt(project, "dashboard.max-scan-rows", 50_000),
                                parseBytes(
                                        get(
                                                project,
                                                "dashboard.max-attachment-preview-size",
                                                "10MB"))));
        return new AgentConfiguration(catalogOptions, projectConfig);
    }

    private static Properties loadRequired(Path path, String description) {
        if (!Files.isRegularFile(path)) {
            throw new ConfigurationException(
                    "Missing " + description + " configuration file: " + path.toAbsolutePath());
        }
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
            return properties;
        } catch (IOException e) {
            throw new ConfigurationException("Unable to read " + path.toAbsolutePath(), e);
        }
    }

    private static SourceConfig source(Properties properties, String name, String defaultPath) {
        return new SourceConfig(
                parseBoolean(properties, "collector." + name + ".enabled", true),
                expandHome(get(properties, "collector." + name + ".path", defaultPath)));
    }

    static Path expandHome(String value) {
        String trimmed = value.trim();
        if (trimmed.equals("~")) {
            return Paths.get(System.getProperty("user.home")).toAbsolutePath().normalize();
        }
        if (trimmed.startsWith("~/")) {
            return Paths.get(System.getProperty("user.home"), trimmed.substring(2))
                    .toAbsolutePath()
                    .normalize();
        }
        return Paths.get(trimmed).toAbsolutePath().normalize();
    }

    static Duration parseDuration(String value) {
        String normalized = value.trim().toLowerCase();
        try {
            if (normalized.startsWith("p")) {
                return Duration.parse(value.trim().toUpperCase());
            }
            if (normalized.endsWith("ms")) {
                return Duration.ofMillis(Long.parseLong(normalized.substring(0, normalized.length() - 2)));
            }
            long amount = Long.parseLong(normalized.substring(0, normalized.length() - 1));
            char unit = normalized.charAt(normalized.length() - 1);
            switch (unit) {
                case 's':
                    return Duration.ofSeconds(amount);
                case 'm':
                    return Duration.ofMinutes(amount);
                case 'h':
                    return Duration.ofHours(amount);
                case 'd':
                    return Duration.ofDays(amount);
                default:
                    throw new IllegalArgumentException("unsupported unit");
            }
        } catch (RuntimeException e) {
            throw new ConfigurationException("Invalid duration: " + value, e);
        }
    }

    static long parseBytes(String value) {
        String normalized = value.trim().toUpperCase().replace(" ", "");
        long multiplier = 1;
        String number = normalized;
        if (normalized.endsWith("KB")) {
            multiplier = 1024L;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("MB")) {
            multiplier = 1024L * 1024L;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("GB")) {
            multiplier = 1024L * 1024L * 1024L;
            number = normalized.substring(0, normalized.length() - 2);
        } else if (normalized.endsWith("B")) {
            number = normalized.substring(0, normalized.length() - 1);
        }
        try {
            return Math.multiplyExact(Long.parseLong(number), multiplier);
        } catch (RuntimeException e) {
            throw new ConfigurationException("Invalid byte size: " + value, e);
        }
    }

    private static boolean parseBoolean(Properties properties, String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        if ("true".equalsIgnoreCase(value.trim())) {
            return true;
        }
        if ("false".equalsIgnoreCase(value.trim())) {
            return false;
        }
        throw new ConfigurationException(key + " must be true or false");
    }

    private static int parseInt(Properties properties, String key, int defaultValue) {
        String value = properties.getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            throw new ConfigurationException(key + " must be an integer", e);
        }
    }

    private static String required(Properties properties, String key) {
        String value = properties.getProperty(key);
        if (value == null || value.trim().isEmpty()) {
            throw new ConfigurationException("Missing required project property: " + key);
        }
        return value.trim();
    }

    private static String get(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key);
        return value == null ? defaultValue : value.trim();
    }

}
