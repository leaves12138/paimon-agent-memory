package org.apache.paimon.agent.source;

import org.apache.paimon.agent.model.AttachmentPayload;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/** Resolves ordered attachments and writes an index-aligned manifest into content_json. */
public final class AttachmentResolver {

    private final ObjectMapper objectMapper;
    private final boolean enabled;
    private final boolean downloadRemote;
    private final long maxBytes;
    private final HttpClient httpClient;

    public AttachmentResolver(
            ObjectMapper objectMapper, boolean enabled, boolean downloadRemote, long maxBytes) {
        this.objectMapper = objectMapper;
        this.enabled = enabled;
        this.downloadRemote = downloadRemote;
        this.maxBytes = maxBytes;
        this.httpClient =
                HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10)).build();
    }

    public ResolvedAttachments resolve(JsonNode rawEvent, List<AttachmentReference> references) {
        JsonNode copied = rawEvent.deepCopy();
        ObjectNode envelope;
        if (copied instanceof ObjectNode) {
            envelope = (ObjectNode) copied;
        } else {
            envelope = objectMapper.createObjectNode();
            envelope.set("raw_event", copied);
        }

        ArrayNode manifest = objectMapper.createArrayNode();
        List<AttachmentPayload> payloads = new ArrayList<>();
        for (int index = 0; index < references.size(); index++) {
            AttachmentReference reference = references.get(index);
            ObjectNode metadata = manifest.addObject();
            metadata.put("index", index);
            putIfPresent(metadata, "file_name", reference.fileName());
            putIfPresent(metadata, "mime_type", reference.mimeType());
            metadata.put("source_kind", reference.kind().name().toLowerCase());
            metadata.put("source_reference", safeReference(reference));

            if (!enabled) {
                metadata.put("status", "disabled");
                payloads.add(AttachmentPayload.missing());
                continue;
            }

            try {
                byte[] bytes = read(reference);
                if (bytes == null) {
                    metadata.put(
                            "status",
                            reference.kind() == AttachmentReference.Kind.REMOTE_URL
                                            && !downloadRemote
                                    ? "remote_not_downloaded"
                                    : "missing");
                    payloads.add(AttachmentPayload.missing());
                } else if (bytes.length > maxBytes) {
                    metadata.put("status", "too_large");
                    metadata.put("size", bytes.length);
                    payloads.add(AttachmentPayload.missing());
                } else {
                    metadata.put("status", "stored");
                    metadata.put("size", bytes.length);
                    metadata.put("sha256", sha256(bytes));
                    payloads.add(AttachmentPayload.of(bytes));
                }
            } catch (AttachmentTooLargeException e) {
                metadata.put("status", "too_large");
                metadata.put("size", e.size);
                payloads.add(AttachmentPayload.missing());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RetryableAttachmentException(
                        "Attachment resolution was interrupted", e);
            } catch (IOException e) {
                throw new RetryableAttachmentException(
                        "Temporary attachment read failure for " + safeReference(reference), e);
            } catch (Exception e) {
                metadata.put("status", "read_error");
                metadata.put("error", e.getClass().getSimpleName());
                payloads.add(AttachmentPayload.missing());
            }
        }
        envelope.set("_paimon_attachments", manifest);
        try {
            return new ResolvedAttachments(objectMapper.writeValueAsString(envelope), payloads);
        } catch (IOException e) {
            throw new IllegalArgumentException("Unable to serialize event JSON", e);
        }
    }

    private byte[] read(AttachmentReference reference) throws Exception {
        switch (reference.kind()) {
            case LOCAL_PATH:
                Path path = resolvePath(reference);
                if (!Files.isRegularFile(path)) {
                    return null;
                }
                long size = Files.size(path);
                if (size > maxBytes) {
                    throw new AttachmentTooLargeException(size);
                }
                try (InputStream input = Files.newInputStream(path)) {
                    return readBounded(input);
                }
            case DATA_URI:
                int comma = reference.value().indexOf(',');
                if (comma < 0) {
                    return null;
                }
                String header = reference.value().substring(0, comma);
                String payload = reference.value().substring(comma + 1);
                return header.toLowerCase(Locale.ROOT).endsWith(";base64")
                        ? decodeBase64(payload)
                        : decodePercentEncoded(payload);
            case BASE64:
                return decodeBase64(reference.value());
            case REMOTE_URL:
                return downloadRemote ? download(reference.value()) : null;
            default:
                throw new IllegalArgumentException("Unknown attachment kind " + reference.kind());
        }
    }

    private Path resolvePath(AttachmentReference reference) {
        String value = reference.value();
        Path path;
        if (value.startsWith("file:")) {
            path = Paths.get(URI.create(value));
        } else if (value.equals("~") || value.startsWith("~/")) {
            String suffix = value.equals("~") ? "" : value.substring(2);
            path = Paths.get(System.getProperty("user.home"), suffix);
        } else {
            path = Paths.get(value);
            if (!path.isAbsolute() && reference.baseDirectory() != null) {
                path = reference.baseDirectory().resolve(path);
            }
        }
        return path.toAbsolutePath().normalize();
    }

    private byte[] download(String value) throws Exception {
        URI uri = URI.create(value);
        if (!("http".equalsIgnoreCase(uri.getScheme())
                || "https".equalsIgnoreCase(uri.getScheme()))) {
            return null;
        }
        HttpRequest request =
                HttpRequest.newBuilder(uri)
                        .timeout(Duration.ofSeconds(30))
                        .GET()
                        .build();
        HttpResponse<InputStream> response =
                httpClient.send(request, HttpResponse.BodyHandlers.ofInputStream());
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            response.body().close();
            if (response.statusCode() >= 500) {
                throw new IOException("Remote attachment returned " + response.statusCode());
            }
            return null;
        }
        String contentLength = response.headers().firstValue("content-length").orElse(null);
        if (contentLength != null) {
            try {
                long declaredSize = Long.parseLong(contentLength);
                if (declaredSize > maxBytes) {
                    response.body().close();
                    throw new AttachmentTooLargeException(declaredSize);
                }
            } catch (NumberFormatException ignored) {
                // Fall back to a bounded streaming read.
            }
        }
        try (InputStream input = response.body()) {
            return readBounded(input);
        }
    }

    private byte[] decodeBase64(String value) throws AttachmentTooLargeException {
        long decodedSize = decodedBase64Size(value);
        if (decodedSize > maxBytes) {
            throw new AttachmentTooLargeException(decodedSize);
        }
        byte[] decoded = Base64.getDecoder().decode(value);
        if (decoded.length > maxBytes) {
            throw new AttachmentTooLargeException(decoded.length);
        }
        return decoded;
    }

    private static long decodedBase64Size(String value) {
        int length = value.length();
        if (length == 0) {
            return 0L;
        }
        int remainder = length % 4;
        long size = ((long) length / 4L) * 3L;
        if (remainder != 0) {
            size += remainder - 1L;
        } else {
            if (value.charAt(length - 1) == '=') {
                size--;
            }
            if (length > 1 && value.charAt(length - 2) == '=') {
                size--;
            }
        }
        return Math.max(0L, size);
    }

    private byte[] decodePercentEncoded(String value) throws AttachmentTooLargeException {
        ByteArrayOutputStream output =
                new ByteArrayOutputStream((int) Math.min(Math.min(maxBytes, 8192L), value.length()));
        for (int index = 0; index < value.length(); ) {
            char current = value.charAt(index);
            if (current == '%') {
                if (index + 2 >= value.length()) {
                    throw new IllegalArgumentException("Incomplete percent escape in data URI");
                }
                int high = Character.digit(value.charAt(index + 1), 16);
                int low = Character.digit(value.charAt(index + 2), 16);
                if (high < 0 || low < 0) {
                    throw new IllegalArgumentException("Invalid percent escape in data URI");
                }
                writeBounded(output, new byte[] {(byte) ((high << 4) | low)});
                index += 3;
            } else {
                int codePoint = value.codePointAt(index);
                byte[] bytes =
                        new String(Character.toChars(codePoint)).getBytes(StandardCharsets.UTF_8);
                writeBounded(output, bytes);
                index += Character.charCount(codePoint);
            }
        }
        return output.toByteArray();
    }

    private byte[] readBounded(InputStream input) throws IOException, AttachmentTooLargeException {
        ByteArrayOutputStream output =
                new ByteArrayOutputStream((int) Math.min(maxBytes, 8192L));
        byte[] buffer = new byte[8192];
        int read;
        while ((read = input.read(buffer)) >= 0) {
            writeBounded(output, buffer, read);
        }
        return output.toByteArray();
    }

    private void writeBounded(ByteArrayOutputStream output, byte[] bytes)
            throws AttachmentTooLargeException {
        writeBounded(output, bytes, bytes.length);
    }

    private void writeBounded(ByteArrayOutputStream output, byte[] bytes, int length)
            throws AttachmentTooLargeException {
        long nextSize = (long) output.size() + length;
        if (nextSize > maxBytes) {
            throw new AttachmentTooLargeException(nextSize);
        }
        output.write(bytes, 0, length);
    }

    private static String safeReference(AttachmentReference reference) {
        if (reference.kind() == AttachmentReference.Kind.BASE64
                || reference.kind() == AttachmentReference.Kind.DATA_URI) {
            return "inline:" + (reference.mimeType() == null ? "unknown" : reference.mimeType());
        }
        if (reference.kind() == AttachmentReference.Kind.REMOTE_URL) {
            try {
                URI value = URI.create(reference.value());
                if (value.getHost() == null) {
                    return "remote:<redacted>";
                }
                return new URI(
                                value.getScheme(),
                                null,
                                value.getHost(),
                                value.getPort(),
                                value.getPath(),
                                null,
                                null)
                        .toASCIIString();
            } catch (IllegalArgumentException | URISyntaxException ignored) {
                return "remote:<redacted>";
            }
        }
        return reference.value();
    }

    private static String sha256(byte[] bytes) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder result = new StringBuilder(digest.length * 2);
            for (byte value : digest) {
                result.append(String.format("%02x", value & 0xff));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static void putIfPresent(ObjectNode node, String field, String value) {
        if (value != null && !value.isEmpty()) {
            node.put(field, value);
        }
    }

    private static final class AttachmentTooLargeException extends Exception {
        private final long size;

        private AttachmentTooLargeException(long size) {
            this.size = size;
        }
    }

    public static final class RetryableAttachmentException extends RuntimeException {
        private RetryableAttachmentException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
