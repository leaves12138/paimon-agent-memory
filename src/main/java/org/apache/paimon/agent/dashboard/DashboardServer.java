package org.apache.paimon.agent.dashboard;

import org.apache.paimon.agent.config.DashboardConfig;
import org.apache.paimon.agent.config.ProjectConfig;
import org.apache.paimon.agent.service.CollectorStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermission;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/** Loopback-only HTTP dashboard for the two chat tables. */
public final class DashboardServer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DashboardServer.class);
    private static final int MAX_QUERY_BYTES = 4 * 1024;
    private static final int MAX_FILTER_CHARS = 512;
    private static final int MAX_CONTENT_CHARS = 2_000_000;
    private static final Set<String> SAFE_INLINE_IMAGES =
            Set.of("image/png", "image/jpeg", "image/gif", "image/webp", "image/avif");

    private final ProjectConfig project;
    private final DashboardConfig config;
    private final DashboardDataStore dataStore;
    private final Supplier<CollectorStatus> collectorStatus;
    private final ObjectMapper objectMapper;
    private final HttpServer server;
    private final ThreadPoolExecutor executor;
    private final Semaphore scanSlots = new Semaphore(2);
    private final Semaphore attachmentSlot = new Semaphore(1);
    private final CountDownLatch stopped = new CountDownLatch(1);
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    public DashboardServer(
            ProjectConfig project,
            DashboardDataStore dataStore,
            Supplier<CollectorStatus> collectorStatus,
            ObjectMapper objectMapper,
            Path dataDirectory)
            throws IOException {
        this.project = project;
        this.config = project.dashboard();
        this.dataStore = dataStore;
        this.collectorStatus = collectorStatus;
        this.objectMapper = objectMapper;

        InetAddress bindAddress = InetAddress.getByName(config.host());
        if (!bindAddress.isLoopbackAddress()) {
            throw new IOException("Dashboard must bind to a loopback address");
        }
        HttpServer createdServer =
                HttpServer.create(new InetSocketAddress(bindAddress, config.port()), 32);
        ThreadPoolExecutor createdExecutor =
                new ThreadPoolExecutor(
                        4,
                        4,
                        0L,
                        TimeUnit.MILLISECONDS,
                        new ArrayBlockingQueue<>(16),
                        new DashboardThreadFactory(),
                        new ThreadPoolExecutor.AbortPolicy());
        try {
            createdServer.setExecutor(createdExecutor);
            createdServer.createContext("/", this::handle);
            preparePrivateDataDirectory(dataDirectory);
        } catch (IOException | RuntimeException failure) {
            createdServer.stop(0);
            createdExecutor.shutdownNow();
            throw failure;
        }
        this.server = createdServer;
        this.executor = createdExecutor;
    }

    public URI start() {
        if (!started.compareAndSet(false, true)) {
            throw new IllegalStateException("Dashboard server has already been started");
        }
        server.start();
        URI address = address();
        LOG.info("Dashboard listening at {}", address);
        return address;
    }

    public URI address() {
        return URI.create(
                "http://"
                        + configuredHostForUrl()
                        + ':'
                        + server.getAddress().getPort()
                        + '/');
    }

    public void await() throws InterruptedException {
        stopped.await();
    }

    private void handle(HttpExchange exchange) {
        try {
            addSecurityHeaders(exchange.getResponseHeaders());
            if (!requestIsLocal(exchange)) {
                sendError(exchange, 403, "Request rejected");
                return;
            }
            String method = exchange.getRequestMethod();
            if (!"GET".equals(method) && !"HEAD".equals(method)) {
                exchange.getResponseHeaders().set("Allow", "GET, HEAD");
                sendError(exchange, 405, "Only GET and HEAD are supported");
                return;
            }
            if (requestHasBody(exchange)) {
                sendError(exchange, 400, "Request bodies are not supported");
                return;
            }

            String path = exchange.getRequestURI().getPath();
            if (path == null || path.indexOf('\\') >= 0 || path.indexOf('\0') >= 0) {
                sendError(exchange, 400, "Invalid request path");
                return;
            }
            switch (path) {
                case "/":
                case "/index.html":
                    sendResource(exchange, "/dashboard/index.html", "text/html; charset=utf-8");
                    return;
                case "/dashboard.css":
                    sendResource(exchange, "/dashboard/dashboard.css", "text/css; charset=utf-8");
                    return;
                case "/dashboard.js":
                    sendResource(
                            exchange,
                            "/dashboard/dashboard.js",
                            "text/javascript; charset=utf-8");
                    return;
                case "/api/overview":
                    handleOverview(exchange);
                    return;
                case "/api/sessions":
                    handleSessions(exchange);
                    return;
                case "/api/messages":
                    handleMessages(exchange);
                    return;
                case "/api/messages/detail":
                    handleMessageDetail(exchange);
                    return;
                case "/api/attachments":
                    handleAttachment(exchange);
                    return;
                default:
                    sendError(exchange, 404, "Not found");
            }
        } catch (BadRequestException error) {
            try {
                sendError(exchange, 400, error.getMessage());
            } catch (IOException sendFailure) {
                LOG.debug("Unable to send dashboard validation response", sendFailure);
            }
        } catch (DashboardScanLimitExceededException error) {
            try {
                sendError(exchange, 422, error.getMessage());
            } catch (IOException sendFailure) {
                LOG.debug("Unable to send dashboard scan-limit response", sendFailure);
            }
        } catch (DashboardAttachmentTooLargeException error) {
            try {
                sendError(exchange, 413, "Attachment exceeds the dashboard preview limit");
            } catch (IOException sendFailure) {
                LOG.debug("Unable to send dashboard attachment-limit response", sendFailure);
            }
        } catch (DashboardBusyException error) {
            try {
                exchange.getResponseHeaders().set("Retry-After", "1");
                sendError(exchange, 503, "Dashboard is busy; retry shortly");
            } catch (IOException sendFailure) {
                LOG.debug("Unable to send dashboard busy response", sendFailure);
            }
        } catch (Throwable error) {
            String requestId = Long.toUnsignedString(new SecureRandom().nextLong(), 36);
            LOG.error("Dashboard request {} failed for route {}", requestId, safeRoute(exchange), error);
            try {
                sendError(exchange, 500, "Request failed (request ID " + requestId + ")");
            } catch (IOException sendFailure) {
                LOG.debug("Unable to send dashboard error response", sendFailure);
            }
        } finally {
            exchange.close();
        }
    }

    private void handleOverview(HttpExchange exchange) throws Exception {
        Map<String, List<String>> values = parameters(exchange);
        requireOnly(values, Set.of("refresh"));
        refreshIfRequested(values);
        DashboardOverview uploaded = withScanSlot(dataStore::overview);
        CollectorStatus status = collectorStatus.get();
        long durablePending = uploaded.getPendingSessionCount();
        long pendingSessions = Math.max(durablePending, status.pendingSessions());

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("database", project.database());
        response.put("sessionsTable", project.sessionsTable());
        response.put("messagesTable", project.messagesTable());
        response.put("defaultPageSize", config.pageSize());
        response.put("maxPageSize", config.maxPageSize());
        response.put("uploadedSessions", Math.max(0L, uploaded.getSessionCount() - durablePending));
        response.put("uploadedMessages", uploaded.getMessageCount());
        response.put("pendingSessions", pendingSessions);
        response.put("pendingMessages", status.pendingMessages());
        response.put("activeSessions", uploaded.getActiveSessionCount());
        response.put("archivedSessions", uploaded.getArchivedSessionCount());
        response.put("sessionsBySource", uploaded.getSessionCountBySource());
        response.put("messagesBySource", uploaded.getMessageCountBySource());
        response.put("overviewTruncated", uploaded.isTruncated());
        response.put("sessionCountTruncated", uploaded.isSessionCountTruncated());
        response.put("messageCountTruncated", uploaded.isMessageCountTruncated());
        response.put("lastIngestedAt", instant(uploaded.getLastIngestedAt()));
        response.put("collectorRunning", status.running());
        response.put("collectorStartedAt", instant(status.startedAt()));
        response.put("lastScanAt", instant(status.lastScanAt()));
        response.put("lastCommitAt", instant(status.lastCommitAt()));
        response.put("lastErrorAt", instant(status.lastErrorAt()));
        response.put("lastError", status.lastError());
        response.put("generatedAt", Instant.now().toString());
        sendJson(exchange, 200, response);
    }

    private void handleSessions(HttpExchange exchange) throws Exception {
        Map<String, List<String>> values = parameters(exchange);
        requireOnly(
                values,
                Set.of("page", "pageSize", "sourceType", "query", "archived", "refresh"));
        int page = positiveInt(single(values, "page", "1"), "page");
        int pageSize = pageSize(values);
        String sourceType = optionalFilter(values, "sourceType");
        String search = optionalFilter(values, "query");
        Boolean archived = optionalBoolean(values, "archived");
        refreshIfRequested(values);
        DashboardPage<DashboardSession> result =
                withScanSlot(
                        () ->
                                dataStore.listSessions(
                                        new SessionQuery(
                                                sourceType,
                                                search,
                                                archived,
                                                page,
                                                pageSize)));
        List<Map<String, Object>> items = new ArrayList<>();
        for (DashboardSession session : result.getItems()) {
            items.add(session(session));
        }
        sendJson(exchange, 200, page(result, items));
    }

    private void handleMessages(HttpExchange exchange) throws Exception {
        Map<String, List<String>> values = parameters(exchange);
        requireOnly(
                values,
                Set.of(
                        "page",
                        "pageSize",
                        "sourceType",
                        "sessionId",
                        "role",
                        "eventType",
                        "query",
                        "refresh"));
        int page = positiveInt(single(values, "page", "1"), "page");
        int pageSize = pageSize(values);
        String sourceType = optionalFilter(values, "sourceType");
        String sessionId = optionalFilter(values, "sessionId");
        String role = optionalFilter(values, "role");
        String eventType = optionalFilter(values, "eventType");
        String search = optionalFilter(values, "query");
        refreshIfRequested(values);
        DashboardPage<DashboardMessage> result =
                withScanSlot(
                        () ->
                                dataStore.listMessages(
                                        new MessageQuery(
                                                sourceType,
                                                sessionId,
                                                role,
                                                eventType,
                                                search,
                                                page,
                                                pageSize)));
        List<Map<String, Object>> items = new ArrayList<>();
        for (DashboardMessage message : result.getItems()) {
            items.add(message(message));
        }
        sendJson(exchange, 200, page(result, items));
    }

    private void refreshIfRequested(Map<String, List<String>> values)
            throws BadRequestException {
        if (Boolean.TRUE.equals(optionalBoolean(values, "refresh"))) {
            dataStore.invalidate();
        }
    }

    private void handleMessageDetail(HttpExchange exchange) throws Exception {
        Map<String, List<String>> values = parameters(exchange);
        requireOnly(values, Set.of("sourceType", "sessionId", "messageId", "sequenceNo"));
        String sourceType = requiredFilter(values, "sourceType");
        String sessionId = requiredFilter(values, "sessionId");
        String messageId = requiredFilter(values, "messageId");
        long sequenceNo = nonNegativeLong(required(values, "sequenceNo"), "sequenceNo");
        Optional<DashboardMessageDetail> detail =
                withScanSlot(
                        () ->
                                dataStore.messageDetail(
                                        sourceType, sessionId, messageId, sequenceNo));
        if (!detail.isPresent()) {
            sendError(exchange, 404, "Message not found");
            return;
        }
        sendJson(exchange, 200, detail(detail.get()));
    }

    private void handleAttachment(HttpExchange exchange) throws Exception {
        Map<String, List<String>> values = parameters(exchange);
        requireOnly(
                values,
                Set.of("sourceType", "sessionId", "messageId", "sequenceNo", "index"));
        String sourceType = requiredFilter(values, "sourceType");
        String sessionId = requiredFilter(values, "sessionId");
        String messageId = requiredFilter(values, "messageId");
        long sequenceNo = nonNegativeLong(required(values, "sequenceNo"), "sequenceNo");
        int index = nonNegativeInt(required(values, "index"), "index");
        if (exchange.getRequestHeaders().containsKey("Range")) {
            throw new BadRequestException("Range requests are not supported");
        }
        if (!attachmentSlot.tryAcquire()) {
            exchange.getResponseHeaders().set("Retry-After", "1");
            sendError(exchange, 429, "Another attachment is being read; retry shortly");
            return;
        }
        try {
            Optional<AttachmentData> attachment =
                    dataStore.attachment(
                            sourceType,
                            sessionId,
                            messageId,
                            sequenceNo,
                            index,
                            config.maxAttachmentPreviewBytes());
            if (!attachment.isPresent()) {
                sendError(exchange, 404, "Attachment not found");
                return;
            }
            sendAttachment(exchange, attachment.get(), index);
        } finally {
            attachmentSlot.release();
        }
    }

    private Map<String, Object> session(DashboardSession session) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("sourceType", session.getSourceType());
        value.put("sessionId", session.getSessionId());
        value.put("title", session.getTitle());
        value.put("cwd", session.getCwd());
        value.put("archived", session.isArchived());
        value.put("sourcePath", session.getSourcePath());
        value.put("sourceCursor", session.getSourceCursor());
        value.put("lastCommitId", session.getLastCommitId());
        value.put("pendingCommitId", session.getPendingCommitId());
        value.put("pendingCursor", session.getPendingCursor());
        value.put("createdAt", instant(session.getCreatedAt()));
        value.put("updatedAt", instant(session.getUpdatedAt()));
        value.put("lastMessageAt", instant(session.getLastMessageAt()));
        value.put("ingestedAt", instant(session.getIngestedAt()));
        value.put("storageStatus", session.getStorageStatus().apiValue());
        return value;
    }

    private Map<String, Object> message(DashboardMessage message) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("messageId", message.getMessageId());
        value.put("sourceType", message.getSourceType());
        value.put("sessionId", message.getSessionId());
        value.put("sequenceNo", message.getSequenceNo());
        value.put("role", message.getRole());
        value.put("eventType", message.getEventType());
        value.put("contentPreview", message.getContentPreview());
        value.put("contentLength", message.getContentLength());
        value.put("createdAt", instant(message.getCreatedAt()));
        value.put("ingestedAt", instant(message.getIngestedAt()));
        value.put("storageStatus", message.getStorageStatus().apiValue());
        return value;
    }

    private Map<String, Object> detail(DashboardMessageDetail detail) {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("messageId", detail.getMessageId());
        value.put("sourceType", detail.getSourceType());
        value.put("sessionId", detail.getSessionId());
        value.put("sequenceNo", detail.getSequenceNo());
        value.put("role", detail.getRole());
        value.put("eventType", detail.getEventType());
        String content = detail.getContentJson();
        boolean truncated = content != null && content.length() > MAX_CONTENT_CHARS;
        value.put("contentJson", truncated ? content.substring(0, MAX_CONTENT_CHARS) : content);
        value.put("contentTruncated", truncated);
        value.put("createdAt", instant(detail.getCreatedAt()));
        value.put("ingestedAt", instant(detail.getIngestedAt()));
        value.put("storageStatus", detail.getStorageStatus().apiValue());
        List<Map<String, Object>> attachments = new ArrayList<>();
        for (DashboardAttachment attachment : detail.getAttachments()) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("index", attachment.getIndex());
            item.put("present", attachment.isPresent());
            item.put("size", attachment.getSize());
            item.put("mimeType", attachment.getMimeType());
            item.put("fileName", attachment.getFileName());
            item.put("status", attachment.getStatus());
            item.put("sha256", attachment.getSha256());
            if (attachment.isPresent()
                    && attachment.getSize() >= 0
                    && attachment.getSize() <= config.maxAttachmentPreviewBytes()) {
                item.put("previewUrl", attachmentUrl(detail, attachment.getIndex()));
            }
            attachments.add(item);
        }
        value.put("attachments", attachments);
        return value;
    }

    private String attachmentUrl(DashboardMessageDetail detail, int index) {
        return "/api/attachments?sourceType="
                + encode(detail.getSourceType())
                + "&sessionId="
                + encode(detail.getSessionId())
                + "&messageId="
                + encode(detail.getMessageId())
                + "&sequenceNo="
                + detail.getSequenceNo()
                + "&index="
                + index;
    }

    private static Map<String, Object> page(
            DashboardPage<?> page, List<Map<String, Object>> items) {
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("items", items);
        response.put("page", page.getPage());
        response.put("pageSize", page.getPageSize());
        response.put("total", page.getTotal());
        response.put("hasMore", page.isHasMore());
        response.put("truncated", page.isTruncated());
        return response;
    }

    private void sendAttachment(HttpExchange exchange, AttachmentData attachment, int index)
            throws IOException {
        byte[] bytes = attachment.getBytes();
        String requestedMime = safeMime(attachment.getMimeType());
        String detectedMime = detectImageMime(bytes);
        boolean inline = detectedMime != null && SAFE_INLINE_IMAGES.contains(detectedMime);
        String contentType = inline ? detectedMime : "application/octet-stream";
        String fileName = safeFileName(attachment.getFileName(), index);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders()
                .set(
                        "Content-Disposition",
                        (inline ? "inline" : "attachment") + "; filename=\"" + fileName + "\"");
        if (!inline && !"application/octet-stream".equals(requestedMime)) {
            exchange.getResponseHeaders().set("X-Original-Content-Type", requestedMime);
        }
        sendBytes(exchange, 200, bytes);
    }

    private static String detectImageMime(byte[] bytes) {
        if (bytes.length >= 8
                && (bytes[0] & 0xff) == 0x89
                && bytes[1] == 'P'
                && bytes[2] == 'N'
                && bytes[3] == 'G') {
            return "image/png";
        }
        if (bytes.length >= 3
                && (bytes[0] & 0xff) == 0xff
                && (bytes[1] & 0xff) == 0xd8
                && (bytes[2] & 0xff) == 0xff) {
            return "image/jpeg";
        }
        if (bytes.length >= 6) {
            String prefix = new String(bytes, 0, 6, StandardCharsets.US_ASCII);
            if ("GIF87a".equals(prefix) || "GIF89a".equals(prefix)) {
                return "image/gif";
            }
        }
        if (bytes.length >= 12
                && "RIFF".equals(new String(bytes, 0, 4, StandardCharsets.US_ASCII))
                && "WEBP".equals(new String(bytes, 8, 4, StandardCharsets.US_ASCII))) {
            return "image/webp";
        }
        if (bytes.length >= 12
                && "ftyp".equals(new String(bytes, 4, 4, StandardCharsets.US_ASCII))) {
            String brand = new String(bytes, 8, 4, StandardCharsets.US_ASCII);
            if ("avif".equals(brand) || "avis".equals(brand)) {
                return "image/avif";
            }
        }
        return null;
    }

    private void sendResource(HttpExchange exchange, String resource, String contentType)
            throws IOException {
        byte[] body;
        try (InputStream input = DashboardServer.class.getResourceAsStream(resource)) {
            if (input == null) {
                sendError(exchange, 404, "Not found");
                return;
            }
            body = readBounded(input, 2 * 1024 * 1024);
        }
        exchange.getResponseHeaders().set("Content-Type", contentType);
        sendBytes(exchange, 200, body);
    }

    private void sendJson(HttpExchange exchange, int status, Object value) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        sendBytes(exchange, status, objectMapper.writeValueAsBytes(value));
    }

    private void sendError(HttpExchange exchange, int status, String message) throws IOException {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("error", message);
        body.put("status", status);
        sendJson(exchange, status, body);
    }

    private static void sendBytes(HttpExchange exchange, int status, byte[] body)
            throws IOException {
        boolean head = "HEAD".equals(exchange.getRequestMethod());
        if (head) {
            exchange.getResponseHeaders().set("Content-Length", Integer.toString(body.length));
            exchange.sendResponseHeaders(status, -1);
            return;
        }
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            output.write(body);
        }
    }

    private boolean requestIsLocal(HttpExchange exchange) {
        InetAddress remote = exchange.getRemoteAddress().getAddress();
        if (remote == null || !remote.isLoopbackAddress()) {
            return false;
        }
        List<String> hosts = exchange.getRequestHeaders().get("Host");
        if (hosts == null || hosts.size() != 1 || !isAllowedHostHeader(hosts.get(0))) {
            return false;
        }
        String requestHost = hosts.get(0);
        List<String> origins = exchange.getRequestHeaders().get("Origin");
        if (origins != null
                && (origins.size() != 1
                        || !origins.get(0).equalsIgnoreCase("http://" + requestHost))) {
            return false;
        }
        List<String> fetchSites = exchange.getRequestHeaders().get("Sec-Fetch-Site");
        return fetchSites == null
                || (fetchSites.size() == 1
                        && ("same-origin".equals(fetchSites.get(0))
                                || "none".equals(fetchSites.get(0))));
    }

    private boolean isAllowedHostHeader(String host) {
        return host.equalsIgnoreCase(expectedHostHeader())
                || host.equalsIgnoreCase("localhost:" + server.getAddress().getPort());
    }

    private String expectedHostHeader() {
        return configuredHostForUrl() + ':' + server.getAddress().getPort();
    }

    private String configuredHostForUrl() {
        return config.host().indexOf(':') >= 0 ? '[' + config.host() + ']' : config.host();
    }

    private static boolean requestHasBody(HttpExchange exchange) {
        String length = exchange.getRequestHeaders().getFirst("Content-Length");
        if (length != null && !"0".equals(length.trim())) {
            return true;
        }
        return exchange.getRequestHeaders().containsKey("Transfer-Encoding");
    }

    private static void addSecurityHeaders(Headers headers) {
        headers.set(
                "Content-Security-Policy",
                "default-src 'none'; script-src 'self'; style-src 'self'; img-src 'self' blob:; "
                        + "connect-src 'self'; object-src 'none'; base-uri 'none'; form-action 'none'; "
                        + "frame-ancestors 'none'; worker-src 'none'");
        headers.set("X-Content-Type-Options", "nosniff");
        headers.set("Referrer-Policy", "no-referrer");
        headers.set("X-Frame-Options", "DENY");
        headers.set("Cross-Origin-Resource-Policy", "same-origin");
        headers.set("Cross-Origin-Opener-Policy", "same-origin");
        headers.set("Permissions-Policy", "camera=(), microphone=(), geolocation=()");
        headers.set("Cache-Control", "private, no-store, max-age=0");
        headers.set("Pragma", "no-cache");
    }

    private Map<String, List<String>> parameters(HttpExchange exchange) {
        String raw = exchange.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) {
            return Collections.emptyMap();
        }
        if (raw.getBytes(StandardCharsets.UTF_8).length > MAX_QUERY_BYTES) {
            throw new BadRequestException("Query string is too long");
        }
        Map<String, List<String>> result = new LinkedHashMap<>();
        for (String pair : raw.split("&", -1)) {
            int equals = pair.indexOf('=');
            String rawName = equals < 0 ? pair : pair.substring(0, equals);
            String rawValue = equals < 0 ? "" : pair.substring(equals + 1);
            String name = decode(rawName);
            String value = decode(rawValue);
            result.computeIfAbsent(name, ignored -> new ArrayList<>()).add(value);
        }
        for (Map.Entry<String, List<String>> entry : result.entrySet()) {
            if (entry.getValue().size() != 1) {
                throw new BadRequestException("Duplicate query parameter: " + entry.getKey());
            }
        }
        return result;
    }

    private static void requireOnly(Map<String, List<String>> values, Set<String> allowed) {
        for (String key : values.keySet()) {
            if (!allowed.contains(key)) {
                throw new BadRequestException("Unknown query parameter: " + key);
            }
        }
    }

    private int pageSize(Map<String, List<String>> values) {
        int value = positiveInt(single(values, "pageSize", Integer.toString(config.pageSize())), "pageSize");
        if (value > config.maxPageSize()) {
            throw new BadRequestException(
                    "pageSize exceeds dashboard.max-page-size=" + config.maxPageSize());
        }
        return value;
    }

    private static String optionalFilter(Map<String, List<String>> values, String key) {
        String value = single(values, key, null);
        if (value == null || value.trim().isEmpty()) {
            return null;
        }
        return validateFilter(value.trim(), key);
    }

    private static String requiredFilter(Map<String, List<String>> values, String key) {
        String value = required(values, key).trim();
        if (value.isEmpty()) {
            throw new BadRequestException(key + " must not be empty");
        }
        return validateFilter(value, key);
    }

    private static String validateFilter(String value, String key) {
        if (value.length() > MAX_FILTER_CHARS) {
            throw new BadRequestException(key + " is too long");
        }
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            if (Character.isISOControl(character)) {
                throw new BadRequestException(key + " contains control characters");
            }
        }
        return value;
    }

    private static Boolean optionalBoolean(Map<String, List<String>> values, String key) {
        String value = single(values, key, null);
        if (value == null || value.isEmpty()) {
            return null;
        }
        if ("true".equals(value)) {
            return true;
        }
        if ("false".equals(value)) {
            return false;
        }
        throw new BadRequestException(key + " must be true or false");
    }

    private static String required(Map<String, List<String>> values, String key) {
        String value = single(values, key, null);
        if (value == null) {
            throw new BadRequestException("Missing query parameter: " + key);
        }
        return value;
    }

    private static String single(Map<String, List<String>> values, String key, String fallback) {
        List<String> valuesForKey = values.get(key);
        return valuesForKey == null ? fallback : valuesForKey.get(0);
    }

    private static int positiveInt(String value, String key) {
        int parsed = nonNegativeInt(value, key);
        if (parsed == 0) {
            throw new BadRequestException(key + " must be greater than zero");
        }
        return parsed;
    }

    private static int nonNegativeInt(String value, String key) {
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 0) {
                throw new BadRequestException(key + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new BadRequestException(key + " must be an integer");
        }
    }

    private static long nonNegativeLong(String value, String key) {
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 0) {
                throw new BadRequestException(key + " must not be negative");
            }
            return parsed;
        } catch (NumberFormatException error) {
            throw new BadRequestException(key + " must be an integer");
        }
    }

    private <T> T withScanSlot(CheckedSupplier<T> operation) throws Exception {
        if (!scanSlots.tryAcquire()) {
            throw new DashboardBusyException();
        }
        try {
            return operation.get();
        } finally {
            scanSlots.release();
        }
    }

    private static String instant(Instant value) {
        return value == null ? null : value.toString();
    }

    private static String safeRoute(HttpExchange exchange) {
        try {
            return exchange.getRequestURI().getPath();
        } catch (RuntimeException ignored) {
            return "unknown";
        }
    }

    private static String safeMime(String value) {
        if (value == null) {
            return "application/octet-stream";
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return normalized.matches("[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+")
                ? normalized
                : "application/octet-stream";
    }

    private static String safeFileName(String value, int index) {
        String fallback = "attachment-" + index;
        if (value == null || value.trim().isEmpty()) {
            return fallback;
        }
        String safe = value.replaceAll("[^A-Za-z0-9._-]", "_");
        if (safe.isEmpty() || ".".equals(safe) || "..".equals(safe)) {
            return fallback;
        }
        return safe.length() <= 120 ? safe : safe.substring(0, 120);
    }

    private static String decode(String value) {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (IllegalArgumentException | IOException error) {
            throw new BadRequestException("Invalid query encoding");
        }
    }

    private static String encode(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name());
        } catch (IOException impossible) {
            throw new IllegalStateException("UTF-8 is unavailable", impossible);
        }
    }

    private static byte[] readBounded(InputStream input, int maximum) throws IOException {
        byte[] buffer = new byte[8192];
        java.io.ByteArrayOutputStream output = new java.io.ByteArrayOutputStream();
        int total = 0;
        int count;
        while ((count = input.read(buffer)) >= 0) {
            total += count;
            if (total > maximum) {
                throw new IOException("Dashboard resource exceeds the packaged size limit");
            }
            output.write(buffer, 0, count);
        }
        return output.toByteArray();
    }

    private static Path preparePrivateDataDirectory(Path directory) throws IOException {
        Path value = directory.toAbsolutePath().normalize();
        if (Files.isSymbolicLink(value)) {
            throw new IOException("Dashboard data directory must not be a symbolic link: " + value);
        }
        Files.createDirectories(value);
        if (!Files.isDirectory(value, LinkOption.NOFOLLOW_LINKS)) {
            throw new IOException("Dashboard data directory is not a directory: " + value);
        }
        setPermissions(value, EnumSet.of(PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE));
        return value.toRealPath();
    }

    private static void setPermissions(Path path, Set<PosixFilePermission> permissions) {
        try {
            Files.setPosixFilePermissions(path, permissions);
        } catch (IOException | UnsupportedOperationException ignored) {
            // umask 077 remains the fallback on filesystems without POSIX permissions.
        }
    }

    @Override
    public void close() throws Exception {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        server.stop(0);
        executor.shutdown();
        boolean terminated = false;
        InterruptedException interrupted = null;
        try {
            terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            interrupted = error;
        }
        if (!terminated) {
            executor.shutdownNow();
            try {
                terminated = executor.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException error) {
                if (interrupted == null) {
                    interrupted = error;
                } else {
                    interrupted.addSuppressed(error);
                }
            }
        }
        Exception failure = interrupted;
        if (terminated) {
            try {
                dataStore.close();
            } catch (Exception error) {
                if (failure == null) {
                    failure = error;
                } else {
                    failure.addSuppressed(error);
                }
            }
        } else {
            IOException terminationFailure =
                    new IOException(
                            "Dashboard requests did not stop; the data store was left open to avoid a concurrent close");
            if (failure == null) {
                failure = terminationFailure;
            } else {
                failure.addSuppressed(terminationFailure);
            }
        }
        stopped.countDown();
        if (interrupted != null) {
            Thread.currentThread().interrupt();
        }
        if (failure != null) {
            throw failure;
        }
    }

    private interface CheckedSupplier<T> {
        T get() throws Exception;
    }

    private static final class DashboardThreadFactory implements ThreadFactory {
        private final AtomicInteger sequence = new AtomicInteger();

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread =
                    new Thread(runnable, "paimon-agent-dashboard-" + sequence.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        }
    }

    private static final class BadRequestException extends IllegalArgumentException {
        private BadRequestException(String message) {
            super(message);
        }
    }

    private static final class DashboardBusyException extends RuntimeException {}
}
