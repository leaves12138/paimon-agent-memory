package org.apache.paimon.agent.dashboard;

import org.apache.paimon.agent.config.DashboardConfig;
import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.config.SourceConfig;
import org.apache.paimon.agent.service.CollectorStatus;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.net.URI;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;

class DashboardServerTest {

    private static final Instant CREATED_AT = Instant.parse("2026-07-13T01:02:03Z");
    private static final Instant INGESTED_AT = Instant.parse("2026-07-13T01:03:04Z");
    private static final byte[] PNG =
            Base64.getDecoder()
                    .decode(
                            "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=");

    @TempDir Path tempDir;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final HttpClient httpClient =
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private DashboardServer server;
    private FakeDashboardDataStore dataStore;
    private URI address;

    @AfterEach
    void closeServer() throws Exception {
        if (server != null) {
            server.close();
        }
    }

    @Test
    void servesStaticResourcesSecurityHeadersAndOverviewWithoutAuthentication() throws Exception {
        startServer();

        HttpResponse<byte[]> index = request("GET", "");
        assertThat(index.statusCode()).isEqualTo(200);
        assertThat(index.headers().firstValue("Content-Type"))
                .contains("text/html; charset=utf-8");
        assertThat(new String(index.body(), StandardCharsets.UTF_8))
                .contains("Paimon 对话数据中心")
                .contains("dashboard.css")
                .contains("id=\"session-list\"")
                .contains("id=\"message-list\"")
                .contains("id=\"chat-pane\"")
                .doesNotContain("id=\"sessions-tab\"", "id=\"messages-tab\"");
        assertSecurityHeaders(index);

        HttpResponse<byte[]> css = request("GET", "dashboard.css");
        assertThat(css.statusCode()).isEqualTo(200);
        assertThat(css.headers().firstValue("Content-Type"))
                .contains("text/css; charset=utf-8");
        assertThat(new String(css.body(), StandardCharsets.UTF_8)).contains(":root");

        HttpResponse<byte[]> javascript = request("GET", "dashboard.js");
        assertThat(javascript.statusCode()).isEqualTo(200);
        assertThat(javascript.headers().firstValue("Content-Type"))
                .contains("text/javascript; charset=utf-8");
        assertThat(new String(javascript.body(), StandardCharsets.UTF_8))
                .contains("\"use strict\"")
                .contains("/api")
                .contains("selectSession")
                .contains("loadOlderMessages")
                .contains("refresh=true")
                .contains("params.set(\"conversationOnly\", \"true\")")
                .doesNotContain(
                        "foundVisibleMessage",
                        "MAX_AUTO_HIDDEN_TOOL_PAGES",
                        "Authorization",
                        "dashboard-url",
                        "capabilityToken");

        HttpResponse<byte[]> overview = request("GET", "api/overview");
        assertThat(overview.statusCode()).isEqualTo(200);
        assertSecurityHeaders(overview);
        JsonNode body = json(overview);
        assertThat(body.path("database").asText()).isEqualTo("ai_memory");
        assertThat(body.path("sessionsTable").asText()).isEqualTo("ai_chat_sessions");
        assertThat(body.path("messagesTable").asText()).isEqualTo("ai_chat_messages");
        assertThat(body.path("defaultPageSize").asInt()).isEqualTo(2);
        assertThat(body.path("maxPageSize").asInt()).isEqualTo(5);
        assertThat(body.path("uploadedSessions").asLong()).isEqualTo(3L);
        assertThat(body.path("uploadedMessages").asLong()).isEqualTo(9L);
        assertThat(body.path("pendingSessions").asInt()).isEqualTo(2);
        assertThat(body.path("pendingMessages").asInt()).isEqualTo(3);
        assertThat(body.path("activeSessions").asLong()).isEqualTo(3L);
        assertThat(body.path("archivedSessions").asLong()).isEqualTo(1L);
        assertThat(body.path("sessionsBySource").path("codex").asLong()).isEqualTo(3L);
        assertThat(body.path("messagesBySource").path("claude").asLong()).isEqualTo(4L);
        assertThat(body.path("overviewTruncated").asBoolean()).isFalse();
        assertThat(body.path("sessionCountTruncated").asBoolean()).isFalse();
        assertThat(body.path("messageCountTruncated").asBoolean()).isFalse();
        assertThat(body.path("collectorRunning").asBoolean()).isTrue();
        assertThat(body.path("lastError").asText()).isEqualTo("temporary error");
        assertThat(body.path("generatedAt").asText()).isNotEmpty();

        assertThat(tempDir.resolve("data/dashboard.token")).doesNotExist();
        server.close();
        server = null;
        assertThat(dataStore.closed).isTrue();
    }

    @Test
    void servesSessionMessageDetailAndPngAttachmentApis() throws Exception {
        startServer();

        HttpResponse<byte[]> sessions =
                request(
                        "GET",
                        "api/sessions?page=2&pageSize=3&sourceType=codex&query=demo"
                                + "&archived=false&refresh=true");
        assertThat(sessions.statusCode()).isEqualTo(200);
        JsonNode sessionsBody = json(sessions);
        assertThat(sessionsBody.path("page").asInt()).isEqualTo(2);
        assertThat(sessionsBody.path("pageSize").asInt()).isEqualTo(3);
        assertThat(sessionsBody.path("total").asLong()).isEqualTo(1L);
        assertThat(sessionsBody.path("hasMore").asBoolean()).isFalse();
        assertThat(sessionsBody.path("truncated").asBoolean()).isFalse();
        JsonNode session = sessionsBody.path("items").get(0);
        assertThat(session.path("sourceType").asText()).isEqualTo("codex");
        assertThat(session.path("sessionId").asText()).isEqualTo("session-1");
        assertThat(session.path("title").asText()).isEqualTo("Demo session");
        assertThat(session.path("archived").asBoolean()).isFalse();
        assertThat(session.path("storageStatus").asText()).isEqualTo("uploaded");
        assertThat(dataStore.lastSessionQuery.getSourceType()).isEqualTo("codex");
        assertThat(dataStore.lastSessionQuery.getSearch()).isEqualTo("demo");
        assertThat(dataStore.lastSessionQuery.getArchived()).isFalse();
        assertThat(dataStore.lastSessionQuery.getPage()).isEqualTo(2);
        assertThat(dataStore.lastSessionQuery.getPageSize()).isEqualTo(3);
        assertThat(dataStore.invalidations).isEqualTo(1);

        HttpResponse<byte[]> messages =
                request(
                        "GET",
                        "api/messages?page=1&pageSize=4&sourceType=codex&sessionId=session-1"
                                + "&role=user&eventType=message&query=hello"
                                + "&conversationOnly=true&refresh=true");
        assertThat(messages.statusCode()).isEqualTo(200);
        JsonNode messagesBody = json(messages);
        JsonNode message = messagesBody.path("items").get(0);
        assertThat(message.path("messageId").asText()).isEqualTo("message-1");
        assertThat(message.path("sequenceNo").asLong()).isEqualTo(7L);
        assertThat(message.path("contentPreview").asText()).isEqualTo("hello preview");
        assertThat(message.path("storageStatus").asText()).isEqualTo("uploaded");
        assertThat(dataStore.lastMessageQuery.getSourceType()).isEqualTo("codex");
        assertThat(dataStore.lastMessageQuery.getSessionId()).isEqualTo("session-1");
        assertThat(dataStore.lastMessageQuery.getRole()).isEqualTo("user");
        assertThat(dataStore.lastMessageQuery.getEventType()).isEqualTo("message");
        assertThat(dataStore.lastMessageQuery.getSearch()).isEqualTo("hello");
        assertThat(dataStore.lastMessageQuery.isConversationOnly()).isTrue();
        assertThat(dataStore.invalidations).isEqualTo(2);

        String key =
                "sourceType=codex&sessionId=session-1&messageId=message-1&sequenceNo=7";
        HttpResponse<byte[]> detail = request("GET", "api/messages/detail?" + key);
        assertThat(detail.statusCode()).isEqualTo(200);
        JsonNode detailBody = json(detail);
        assertThat(detailBody.path("contentJson").asText()).isEqualTo("{\"text\":\"hello\"}");
        assertThat(detailBody.path("contentTruncated").asBoolean()).isFalse();
        assertThat(detailBody.path("storageStatus").asText()).isEqualTo("uploaded");
        JsonNode attachment = detailBody.path("attachments").get(0);
        assertThat(attachment.path("present").asBoolean()).isTrue();
        assertThat(attachment.path("mimeType").asText()).isEqualTo("image/png");
        assertThat(attachment.path("fileName").asText()).isEqualTo("pixel demo.png");
        assertThat(attachment.path("previewUrl").asText())
                .contains("/api/attachments?sourceType=codex")
                .contains("messageId=message-1");
        assertThat(dataStore.lastDetailKey).isEqualTo("codex/session-1/message-1/7");

        HttpResponse<byte[]> image = request("GET", "api/attachments?" + key + "&index=0");
        assertThat(image.statusCode()).isEqualTo(200);
        assertThat(image.headers().firstValue("Content-Type")).contains("image/png");
        assertThat(image.headers().firstValue("Content-Disposition"))
                .hasValueSatisfying(
                        value ->
                                assertThat(value)
                                        .contains("inline")
                                        .contains("pixel_demo.png"));
        assertThat(image.body()).isEqualTo(PNG);
        assertThat(dataStore.lastAttachmentKey).isEqualTo("codex/session-1/message-1/7/0");
        assertThat(dataStore.lastAttachmentMaxBytes).isEqualTo(1024L * 1024L);
    }

    @Test
    void rejectsInvalidAndDuplicateQueriesAndUnsupportedMethods() throws Exception {
        startServer();

        assertError(
                request("GET", "api/sessions?page=1&page=2"),
                400,
                "Duplicate query parameter: page");
        assertError(
                request("GET", "api/sessions?unknown=value"),
                400,
                "Unknown query parameter: unknown");
        assertError(
                request("GET", "api/sessions?archived=maybe"),
                400,
                "archived must be true or false");
        assertError(
                request("GET", "api/messages?pageSize=6"),
                400,
                "pageSize exceeds dashboard.max-page-size=5");
        assertError(
                request("GET", "api/messages?refresh=now"),
                400,
                "refresh must be true or false");
        assertError(
                request("GET", "api/messages?conversationOnly=maybe"),
                400,
                "conversationOnly must be true or false");

        HttpResponse<byte[]> post = request("POST", "api/overview");
        assertError(post, 405, "Only GET and HEAD are supported");
        assertThat(post.headers().firstValue("Allow")).contains("GET, HEAD");
        assertSecurityHeaders(post);
    }

    @Test
    void rejectsCrossOriginAndCrossSiteRequests() throws Exception {
        startServer();

        assertError(
                request("GET", "api/overview", "Origin", "http://example.invalid"),
                403,
                "Request rejected");
        assertError(
                request("GET", "api/overview", "Sec-Fetch-Site", "cross-site"),
                403,
                "Request rejected");
    }

    @Test
    void acceptsLocalhostAliasAndRequiresItsMatchingOrigin() throws Exception {
        startServer();
        URI localhostAddress =
                URI.create("http://localhost:" + address.getPort() + '/');

        assertThat(request(localhostAddress, "GET", "").statusCode()).isEqualTo(200);
        assertThat(
                        request(
                                        localhostAddress,
                                        "GET",
                                        "api/overview",
                                        "Origin",
                                        "http://localhost:" + address.getPort())
                                .statusCode())
                .isEqualTo(200);
        assertError(
                request(
                        localhostAddress,
                        "GET",
                        "api/overview",
                        "Origin",
                        address.toString().replaceAll("/$", "")),
                403,
                "Request rejected");
    }

    @Test
    void servesDashboardOnConfiguredIpv6Loopback() throws Exception {
        Assumptions.assumeTrue(ipv6LoopbackAvailable(), "IPv6 loopback is unavailable");
        startServer("::1");

        assertThat(address.toString()).startsWith("http://[::1]:");
        HttpResponse<byte[]> index = request("GET", "");
        assertThat(index.statusCode()).isEqualTo(200);
        assertThat(new String(index.body(), StandardCharsets.UTF_8))
                .contains("Paimon 对话数据中心");
    }

    private void startServer() throws Exception {
        startServer("127.0.0.1");
    }

    private void startServer(String host) throws Exception {
        dataStore = new FakeDashboardDataStore();
        server =
                new DashboardServer(
                        project(host),
                        dataStore,
                        () ->
                                new CollectorStatus(
                                        true,
                                        CREATED_AT,
                                        CREATED_AT.plusSeconds(10),
                                        CREATED_AT.plusSeconds(20),
                                        CREATED_AT.plusSeconds(30),
                                        "temporary error",
                                        2,
                                        3),
                        objectMapper,
                        tempDir.resolve("data"));
        address = server.start();
    }

    private ProjectConfig project(String host) {
        return new ProjectConfig(
                "ai_memory",
                "ai_chat_sessions",
                "ai_chat_messages",
                Duration.ofMinutes(5),
                Duration.ofMinutes(5),
                false,
                "dashboard-server-test",
                new SourceConfig(false, tempDir),
                new SourceConfig(false, tempDir),
                true,
                false,
                1024 * 1024L,
                100,
                100,
                0,
                Duration.ofSeconds(1),
                new DashboardConfig(
                        true,
                        host,
                        freePort(host),
                        2,
                        5,
                        100,
                        1024L * 1024L));
    }

    private static int freePort(String host) {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress(host, 0));
            return socket.getLocalPort();
        } catch (Exception error) {
            throw new AssertionError("Unable to allocate a loopback test port", error);
        }
    }

    private static boolean ipv6LoopbackAvailable() {
        try (ServerSocket socket = new ServerSocket()) {
            socket.bind(new InetSocketAddress("::1", 0));
            return true;
        } catch (Exception ignored) {
            return false;
        }
    }

    private HttpResponse<byte[]> request(String method, String relativePath) throws Exception {
        return request(address, method, relativePath, null, null);
    }

    private HttpResponse<byte[]> request(
            String method, String relativePath, String headerName, String headerValue)
            throws Exception {
        return request(address, method, relativePath, headerName, headerValue);
    }

    private HttpResponse<byte[]> request(URI baseAddress, String method, String relativePath)
            throws Exception {
        return request(baseAddress, method, relativePath, null, null);
    }

    private HttpResponse<byte[]> request(
            URI baseAddress,
            String method,
            String relativePath,
            String headerName,
            String headerValue)
            throws Exception {
        HttpRequest.Builder request =
                HttpRequest.newBuilder(baseAddress.resolve(relativePath))
                        .timeout(Duration.ofSeconds(5));
        if (headerName != null) {
            request.header(headerName, headerValue);
        }
        request.method(method, HttpRequest.BodyPublishers.noBody());
        return httpClient.send(request.build(), HttpResponse.BodyHandlers.ofByteArray());
    }

    private JsonNode json(HttpResponse<byte[]> response) throws Exception {
        return objectMapper.readTree(response.body());
    }

    private void assertError(HttpResponse<byte[]> response, int status, String message)
            throws Exception {
        assertThat(response.statusCode()).isEqualTo(status);
        assertThat(json(response).path("status").asInt()).isEqualTo(status);
        assertThat(json(response).path("error").asText()).contains(message);
    }

    private static void assertSecurityHeaders(HttpResponse<?> response) {
        assertThat(response.headers().firstValue("Content-Security-Policy"))
                .hasValueSatisfying(value -> assertThat(value).contains("default-src 'none'"));
        assertThat(response.headers().firstValue("X-Content-Type-Options")).contains("nosniff");
        assertThat(response.headers().firstValue("X-Frame-Options")).contains("DENY");
        assertThat(response.headers().firstValue("Referrer-Policy")).contains("no-referrer");
        assertThat(response.headers().firstValue("Cache-Control"))
                .hasValueSatisfying(value -> assertThat(value).contains("no-store"));
    }

    private static final class FakeDashboardDataStore implements DashboardDataStore {

        private final AtomicBoolean closed = new AtomicBoolean();
        private SessionQuery lastSessionQuery;
        private MessageQuery lastMessageQuery;
        private String lastDetailKey;
        private String lastAttachmentKey;
        private long lastAttachmentMaxBytes;
        private int invalidations;

        @Override
        public DashboardOverview overview() {
            Map<String, Long> sessions = new LinkedHashMap<>();
            sessions.put("codex", 3L);
            sessions.put("claude", 1L);
            Map<String, Long> messages = new LinkedHashMap<>();
            messages.put("codex", 5L);
            messages.put("claude", 4L);
            return new DashboardOverview(
                    4L, 9L, 3L, 1L, 1L, sessions, messages, INGESTED_AT);
        }

        @Override
        public DashboardPage<DashboardSession> listSessions(SessionQuery query) {
            lastSessionQuery = query;
            DashboardSession session =
                    new DashboardSession(
                            "codex",
                            "session-1",
                            "Demo session",
                            "/tmp/project",
                            false,
                            "/tmp/session.jsonl",
                            "file:10",
                            7L,
                            null,
                            null,
                            CREATED_AT,
                            CREATED_AT.plusSeconds(1),
                            CREATED_AT.plusSeconds(2),
                            INGESTED_AT);
            return new DashboardPage<>(
                    Collections.singletonList(session),
                    query.getPage(),
                    query.getPageSize(),
                    1L);
        }

        @Override
        public DashboardPage<DashboardMessage> listMessages(MessageQuery query) {
            lastMessageQuery = query;
            DashboardMessage message =
                    new DashboardMessage(
                            "message-1",
                            "codex",
                            "session-1",
                            7L,
                            "user",
                            "message",
                            "hello preview",
                            16L,
                            CREATED_AT,
                            INGESTED_AT);
            return new DashboardPage<>(
                    Collections.singletonList(message),
                    query.getPage(),
                    query.getPageSize(),
                    1L);
        }

        @Override
        public Optional<DashboardMessageDetail> messageDetail(
                String sourceType, String sessionId, String messageId, long sequenceNo) {
            lastDetailKey = sourceType + '/' + sessionId + '/' + messageId + '/' + sequenceNo;
            DashboardAttachment attachment =
                    new DashboardAttachment(
                            0,
                            true,
                            PNG.length,
                            "image/png",
                            "pixel demo.png",
                            "stored",
                            "sha256");
            return Optional.of(
                    new DashboardMessageDetail(
                            messageId,
                            sourceType,
                            sessionId,
                            sequenceNo,
                            "user",
                            "message",
                            "{\"text\":\"hello\"}",
                            Collections.singletonList(attachment),
                            CREATED_AT,
                            INGESTED_AT));
        }

        @Override
        public Optional<AttachmentData> attachment(
                String sourceType,
                String sessionId,
                String messageId,
                long sequenceNo,
                int index,
                long maxBytes) {
            lastAttachmentKey =
                    sourceType
                            + '/'
                            + sessionId
                            + '/'
                            + messageId
                            + '/'
                            + sequenceNo
                            + '/'
                            + index;
            lastAttachmentMaxBytes = maxBytes;
            return Optional.of(new AttachmentData(PNG, "image/png", "pixel demo.png"));
        }

        @Override
        public void invalidate() {
            invalidations++;
        }

        @Override
        public void close() {
            closed.set(true);
        }
    }
}
