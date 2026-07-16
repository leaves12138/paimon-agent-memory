package org.apache.paimon.agent;

import org.apache.paimon.agent.config.AgentConfiguration;
import org.apache.paimon.agent.config.ConfigLoader;
import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.dashboard.DashboardDataStore;
import org.apache.paimon.agent.dashboard.DashboardServer;
import org.apache.paimon.agent.dashboard.DashboardRestoreResult;
import org.apache.paimon.agent.dashboard.DashboardSessionRestorer;
import org.apache.paimon.agent.dashboard.LiveDashboardDataStore;
import org.apache.paimon.agent.dashboard.PaimonDashboardDataStore;
import org.apache.paimon.agent.model.ChatSession;
import org.apache.paimon.agent.restore.RestoreOptions;
import org.apache.paimon.agent.restore.RestoreClientProcessGuard;
import org.apache.paimon.agent.restore.RestoreService;
import org.apache.paimon.agent.restore.RestoreSummary;
import org.apache.paimon.agent.restore.RestoreType;
import org.apache.paimon.agent.service.AgentProcessLock;
import org.apache.paimon.agent.service.CollectorService;
import org.apache.paimon.agent.service.CollectorStatus;
import org.apache.paimon.agent.service.PendingBatchStore;
import org.apache.paimon.agent.service.PendingDataSnapshot;
import org.apache.paimon.agent.sink.PaimonChatRepository;
import org.apache.paimon.agent.source.AttachmentResolver;
import org.apache.paimon.agent.source.ConversationSource;
import org.apache.paimon.agent.source.claude.ClaudeConversationSource;
import org.apache.paimon.agent.source.codex.CodexConversationSource;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/** Command-line entry point for the Paimon AI chat collector. */
public final class PaimonAgent {

    private PaimonAgent() {}

    public static void main(String[] args) throws Exception {
        Arguments arguments = Arguments.parse(args);
        if (arguments.help) {
            printHelp();
            return;
        }

        AgentConfiguration configuration =
                ConfigLoader.load(arguments.paimonConfig, arguments.projectConfig);
        ObjectMapper objectMapper = new ObjectMapper();
        if (arguments.command == Command.RESTORE) {
            restore(arguments, configuration, objectMapper);
            return;
        }
        if (arguments.command == Command.DASHBOARD) {
            dashboard(arguments, configuration, objectMapper);
            return;
        }
        if (arguments.command == Command.DASHBOARD_URL) {
            printDashboardUrl(configuration);
            return;
        }

        try (AgentProcessLock ignored =
                AgentProcessLock.acquire(
                        configuration,
                        arguments.projectConfig)) {
            ProjectConfig project = configuration.project();
            AttachmentResolver attachmentResolver =
                    new AttachmentResolver(
                            objectMapper,
                            project.attachmentsEnabled(),
                            project.downloadRemoteAttachments(),
                            project.maxAttachmentBytes());

            List<ConversationSource> sources = new ArrayList<>();
            if (project.codex().enabled()) {
                sources.add(
                        new CodexConversationSource(
                                project.codex().path(), objectMapper, attachmentResolver));
            }
            if (project.claude().enabled()) {
                sources.add(
                        new ClaudeConversationSource(
                                project.claude().path(), objectMapper, attachmentResolver));
            }
            if (sources.isEmpty()) {
                throw new IllegalArgumentException(
                        "At least one conversation source must be enabled");
            }

            PaimonChatRepository repository = new PaimonChatRepository(configuration);
            repository.initialize();
            CollectorService service;
            try {
                String legacyRestWalIdentity =
                        "rest".equals(configuration.catalogOptions().get("metastore"))
                                ? AgentProcessLock.tablePairIdentity(configuration)
                                : null;
                service =
                        new CollectorService(
                                project,
                                sources,
                                repository,
                                new PendingBatchStore(
                                        arguments.dataDirectory.resolve("pending"),
                                        project.collectorId(),
                                        repository.walTablePairIdentity(),
                                        legacyRestWalIdentity));
            } catch (Exception failure) {
                try {
                    repository.close();
                } catch (Exception closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
            if (arguments.runOnce || project.runOnce()) {
                try {
                    service.runOnce();
                } finally {
                    service.close();
                }
                return;
            }

            DashboardServer dashboard = null;
            try {
                if (project.dashboard().enabled()) {
                    dashboard =
                            createDashboard(
                                    configuration,
                                    objectMapper,
                                    arguments.dataDirectory,
                                    service::status,
                                    service::pendingData,
                                    service::commitGeneration);
                    dashboard.start();
                }

                CountDownLatch stopped = new CountDownLatch(1);
                DashboardServer activeDashboard = dashboard;
                Runtime.getRuntime()
                        .addShutdownHook(
                                new Thread(
                                        () -> {
                                            try {
                                                closeDashboardAndCollector(
                                                        activeDashboard, service);
                                            } catch (Exception e) {
                                                e.printStackTrace(System.err);
                                            } finally {
                                                stopped.countDown();
                                            }
                                        },
                                        "paimon-agent-shutdown"));
                service.start();
                stopped.await();
            } catch (Exception failure) {
                try {
                    closeDashboardAndCollector(dashboard, service);
                } catch (Exception closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                throw failure;
            }
        }
    }

    private static void dashboard(
            Arguments arguments, AgentConfiguration configuration, ObjectMapper objectMapper)
            throws Exception {
        DashboardServer dashboard =
                createDashboard(
                        configuration,
                        objectMapper,
                        arguments.dataDirectory,
                        CollectorStatus::offline,
                        PendingDataSnapshot::empty,
                        () -> 0L);
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    try {
                                        dashboard.close();
                                    } catch (Exception e) {
                                        e.printStackTrace(System.err);
                                    } finally {
                                        stopped.countDown();
                                    }
                                },
                                "paimon-agent-dashboard-shutdown"));
        try {
            dashboard.start();
            dashboard.await();
        } catch (Exception failure) {
            try {
                dashboard.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
        stopped.await();
    }

    private static void printDashboardUrl(AgentConfiguration configuration) {
        String host = configuration.project().dashboard().host();
        if (host.indexOf(':') >= 0) {
            host = '[' + host + ']';
        }
        System.out.println(
                "http://"
                        + host
                        + ':'
                        + configuration.project().dashboard().port()
                        + '/');
    }

    private static DashboardServer createDashboard(
            AgentConfiguration configuration,
            ObjectMapper objectMapper,
            Path dataDirectory,
            java.util.function.Supplier<CollectorStatus> collectorStatus,
            java.util.function.Supplier<PendingDataSnapshot> pendingData,
            java.util.function.LongSupplier commitGeneration)
            throws Exception {
        PaimonDashboardDataStore dataStore =
                PaimonDashboardDataStore.open(
                        configuration, configuration.project().dashboard().maxScanRows());
        DashboardDataStore liveDataStore =
                new LiveDashboardDataStore(
                        dataStore,
                        pendingData,
                        commitGeneration,
                        configuration.project().dashboard().maxScanRows());
        try {
            return new DashboardServer(
                    configuration.project(),
                    liveDataStore,
                    collectorStatus,
                    objectMapper,
                    dataDirectory,
                    dashboardSessionRestorer(
                            configuration, objectMapper, dataDirectory, pendingData));
        } catch (Exception failure) {
            try {
                liveDataStore.close();
            } catch (Exception closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static DashboardSessionRestorer dashboardSessionRestorer(
            AgentConfiguration configuration,
            ObjectMapper objectMapper,
            Path dataDirectory,
            java.util.function.Supplier<PendingDataSnapshot> pendingData) {
        return (type, sessionId) -> {
            RestoreClientProcessGuard.requireStopped(type);
            Path target =
                    type == RestoreType.CODEX
                            ? configuration.project().codex().path()
                            : configuration.project().claude().path();
            RestoreOptions options =
                    new RestoreOptions(
                            type,
                            target,
                            dataDirectory,
                            null,
                            sessionId,
                            false);
            try (PaimonChatRepository repository = new PaimonChatRepository(configuration)) {
                repository.initializeForRestore();
                RestoreSummary summary =
                        new RestoreService(repository, objectMapper)
                                .restore(
                                        options,
                                        () -> {
                                            List<ChatSession> pendingSessions =
                                                    new ArrayList<>();
                                            pendingData
                                                    .get()
                                                    .batches()
                                                    .forEach(
                                                            batch ->
                                                                    pendingSessions.add(
                                                                            batch.session()));
                                            return pendingSessions;
                                        },
                                        () -> RestoreClientProcessGuard.requireStopped(type));
                return new DashboardRestoreResult(options.target(), summary);
            }
        };
    }

    private static void closeDashboardAndCollector(
            DashboardServer dashboard, CollectorService collector) throws Exception {
        Exception failure = null;
        if (dashboard != null) {
            try {
                dashboard.close();
            } catch (Exception error) {
                failure = error;
            }
        }
        try {
            collector.close();
        } catch (Exception error) {
            if (failure == null) {
                failure = error;
            } else {
                failure.addSuppressed(error);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void restore(
            Arguments arguments, AgentConfiguration configuration, ObjectMapper objectMapper)
            throws Exception {
        RestoreType type = RestoreType.parse(arguments.restoreType);
        RestoreOptions options =
                new RestoreOptions(
                        type,
                        arguments.restoreTarget,
                        arguments.dataDirectory,
                        arguments.targetProject,
                        arguments.sessionId,
                        arguments.overwrite);
        try (PaimonChatRepository repository = new PaimonChatRepository(configuration)) {
            repository.initializeForRestore();
            RestoreSummary summary =
                    new RestoreService(repository, objectMapper).restore(options);
            System.out.printf(
                    "Restored %d %s session(s) and %d message(s); skipped %d existing or pending session(s).%n",
                    summary.restoredSessions(),
                    type.sourceType(),
                    summary.restoredMessages(),
                    summary.skippedSessions());
            if (summary.restoredSessions() > 0) {
                System.out.printf(
                        "History was written under %s. Restart %s before opening restored sessions.%n",
                        options.target(), type == RestoreType.CODEX ? "Codex" : "Claude Code");
            }
        }
    }

    private static void printHelp() {
        System.out.println("Usage:");
        System.out.println("  paimon-agent [run] [collector options]");
        System.out.println("  paimon-agent dashboard [common options]");
        System.out.println("  paimon-agent dashboard-url [common options]");
        System.out.println("  paimon-agent restore --type codex|claude [restore options]");
        System.out.println();
        System.out.println("Common options:");
        System.out.println("  --paimon-config <path>  Catalog options (default: paimon.properties)");
        System.out.println("  --project-config <path> Collector options (default: project.properties)");
        System.out.println("  --help                   Show this help");
        System.out.println();
        System.out.println("Collector options:");
        System.out.println("  --once                   Scan and commit once, then exit");
        System.out.println();
        System.out.println("Restore options:");
        System.out.println("  --type codex|claude      Required native output format");
        System.out.println("  --target <path>          Client home (default: ~/.codex or ~/.claude)");
        System.out.println("  --target-project <path>  Restored project cwd on this machine");
        System.out.println("  --session-id <id>        Restore only one source session");
        System.out.println("  --overwrite              Replace an existing local session");
        System.out.println("  --data-dir <path>        Restore staging/cache directory (default: data)");
        System.out.println("Stop the destination client before restoring history.");
    }

    private enum Command {
        RUN,
        DASHBOARD,
        DASHBOARD_URL,
        RESTORE
    }

    private static final class Arguments {
        private Command command = Command.RUN;
        private Path paimonConfig = ConfigLoader.DEFAULT_PAIMON_CONFIG;
        private Path projectConfig = ConfigLoader.DEFAULT_PROJECT_CONFIG;
        private Path dataDirectory = Paths.get("data");
        private Path restoreTarget;
        private Path targetProject;
        private String restoreType;
        private String sessionId;
        private boolean overwrite;
        private boolean runOnce;
        private boolean help;

        private static Arguments parse(String[] args) {
            Arguments parsed = new Arguments();
            int start = 0;
            if (args.length > 0 && "restore".equals(args[0])) {
                parsed.command = Command.RESTORE;
                start = 1;
            } else if (args.length > 0 && "dashboard".equals(args[0])) {
                parsed.command = Command.DASHBOARD;
                start = 1;
            } else if (args.length > 0 && "dashboard-url".equals(args[0])) {
                parsed.command = Command.DASHBOARD_URL;
                start = 1;
            } else if (args.length > 0 && "run".equals(args[0])) {
                start = 1;
            }
            for (int index = start; index < args.length; index++) {
                String argument = args[index];
                switch (argument) {
                    case "--paimon-config":
                        parsed.paimonConfig = Paths.get(requireValue(args, ++index, argument));
                        break;
                    case "--project-config":
                        parsed.projectConfig = Paths.get(requireValue(args, ++index, argument));
                        break;
                    case "--data-dir":
                        parsed.dataDirectory = Paths.get(requireValue(args, ++index, argument));
                        break;
                    case "--type":
                        parsed.restoreType = requireValue(args, ++index, argument);
                        break;
                    case "--target":
                        parsed.restoreTarget = Paths.get(requireValue(args, ++index, argument));
                        break;
                    case "--target-project":
                        parsed.targetProject = Paths.get(requireValue(args, ++index, argument));
                        break;
                    case "--session-id":
                        parsed.sessionId = requireValue(args, ++index, argument);
                        break;
                    case "--overwrite":
                        parsed.overwrite = true;
                        break;
                    case "--once":
                        parsed.runOnce = true;
                        break;
                    case "--help":
                    case "-h":
                        parsed.help = true;
                        break;
                    default:
                        throw new IllegalArgumentException("Unknown argument: " + argument);
                }
            }
            parsed.validate();
            return parsed;
        }

        private void validate() {
            if (help) {
                return;
            }
            if (command != Command.RUN && runOnce) {
                throw new IllegalArgumentException("--once is only valid for the collector");
            }
            if (command == Command.RESTORE) {
                if (restoreType == null) {
                    throw new IllegalArgumentException(
                            "restore requires --type codex|claude");
                }
                return;
            }
            if (restoreType != null
                    || restoreTarget != null
                    || targetProject != null
                    || sessionId != null
                    || overwrite) {
                throw new IllegalArgumentException(
                        "Restore options require the restore command");
            }
        }

        private static String requireValue(String[] args, int index, String option) {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[index];
        }
    }
}
