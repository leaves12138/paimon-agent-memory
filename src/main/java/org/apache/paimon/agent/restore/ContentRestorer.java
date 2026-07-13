package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Reverses the attachment placeholders stored in content_json. */
final class ContentRestorer {

    private static final Logger LOG = LoggerFactory.getLogger(ContentRestorer.class);

    private final ObjectMapper objectMapper;

    ContentRestorer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    String restore(ChatMessage message, Path attachmentDirectory) throws IOException {
        return restore(message, attachmentDirectory, attachmentDirectory);
    }

    String restore(
            ChatMessage message,
            Path stagedAttachmentDirectory,
            Path finalAttachmentDirectory)
            throws IOException {
        JsonNode event = objectMapper.readTree(message.contentJson());
        if (!(event instanceof ObjectNode)) {
            return objectMapper.writeValueAsString(event);
        }

        ObjectNode object = (ObjectNode) event;
        JsonNode manifestNode = object.remove("_paimon_attachments");
        if (!(manifestNode instanceof ArrayNode)) {
            return objectMapper.writeValueAsString(object);
        }

        ArrayNode missingAttachments = objectMapper.createArrayNode();
        for (JsonNode metadata : manifestNode) {
            int index = metadata.path("index").asInt(-1);
            if (index < 0) {
                continue;
            }
            String kind = metadata.path("source_kind").asText("");
            String sourceReference = text(metadata, "source_reference");
            String mimeType = text(metadata, "mime_type");
            byte[] bytes = attachmentBytes(message.attachments(), index);
            boolean unavailable = bytes == null && !"remote_url".equals(kind);
            if (unavailable) {
                recordMissingAttachment(missingAttachments, metadata, message, index);
            }
            String replacement = replacement(kind, sourceReference, mimeType, bytes);

            if (("local_path".equals(kind) || "remote_url".equals(kind)) && bytes != null) {
                String fileName =
                        restoredFileName(
                                message,
                                index,
                                text(metadata, "file_name"),
                                mimeType,
                                bytes);
                Path restoredPath =
                        stageAttachment(
                                stagedAttachmentDirectory,
                                finalAttachmentDirectory,
                                fileName,
                                bytes);
                replacement = restoredPath.toAbsolutePath().normalize().toString();
            }

            if (replacement != null) {
                event = replaceExactText(event, "paimon-blob:" + index, replacement);
                if ("local_path".equals(kind) && sourceReference != null) {
                    event = replacePathReference(event, sourceReference, replacement);
                }
            } else if (!unavailable) {
                recordMissingAttachment(missingAttachments, metadata, message, index);
            }
        }
        if (!missingAttachments.isEmpty()) {
            object.set("_paimon_restore_missing_attachments", missingAttachments);
        }
        return objectMapper.writeValueAsString(event);
    }

    private static String replacement(
            String kind, String sourceReference, String mimeType, byte[] bytes) {
        switch (kind) {
            case "data_uri":
                if (bytes == null) {
                    return null;
                }
                return "data:"
                        + (mimeType == null ? "application/octet-stream" : mimeType)
                        + ";base64,"
                        + Base64.getEncoder().encodeToString(bytes);
            case "base64":
                return bytes == null ? null : Base64.getEncoder().encodeToString(bytes);
            case "remote_url":
                return sourceReference;
            case "local_path":
                return sourceReference;
            default:
                return sourceReference;
        }
    }

    private static byte[] attachmentBytes(List<AttachmentPayload> attachments, int index) {
        if (index >= attachments.size() || attachments.get(index).isMissing()) {
            return null;
        }
        return attachments.get(index).bytes();
    }

    private static Path stageAttachment(
            Path stagingDirectory,
            Path finalDirectory,
            String fileName,
            byte[] bytes)
            throws IOException {
        Files.createDirectories(stagingDirectory);
        RestoreFiles.setOwnerOnlyDirectoryPermissions(stagingDirectory);
        Path staged = stagingDirectory.resolve(fileName).normalize();
        Path output = finalDirectory.resolve(fileName).toAbsolutePath().normalize();
        if (!staged.startsWith(stagingDirectory.normalize())
                || !output.startsWith(finalDirectory.toAbsolutePath().normalize())) {
            throw new IOException("Unsafe restored attachment path " + fileName);
        }
        try {
            Files.createFile(staged);
            RestoreFiles.setOwnerOnlyFilePermissions(staged);
        } catch (FileAlreadyExistsException exists) {
            verifyExistingAttachment(staged, bytes);
            return output;
        }

        Path temporary = Files.createTempFile(stagingDirectory, ".attachment-", ".tmp");
        boolean installed = false;
        try {
            Files.write(temporary, bytes, StandardOpenOption.TRUNCATE_EXISTING);
            try (FileChannel channel =
                    FileChannel.open(temporary, StandardOpenOption.WRITE)) {
                channel.force(true);
            }
            RestoreFiles.setOwnerOnlyFilePermissions(temporary);
            try {
                Files.move(
                        temporary,
                        staged,
                        StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException ignored) {
                Files.move(temporary, staged, StandardCopyOption.REPLACE_EXISTING);
            }
            RestoreFiles.setOwnerOnlyFilePermissions(staged);
            installed = true;
            return output;
        } finally {
            Files.deleteIfExists(temporary);
            if (!installed) {
                Files.deleteIfExists(staged);
            }
        }
    }

    private static void verifyExistingAttachment(Path output, byte[] expected) throws IOException {
        if (!Files.isRegularFile(output)
                || Files.size(output) != expected.length
                || !sha256(output).equals(sha256(expected))) {
            throw new IOException(
                    "Refusing to replace an attachment whose content does not match its name: "
                            + output);
        }
    }

    private static String restoredFileName(
            ChatMessage message,
            int index,
            String originalName,
            String mimeType,
            byte[] bytes) {
        String candidate = originalName;
        if (candidate == null || candidate.trim().isEmpty()) {
            candidate = "attachment-" + index + extension(mimeType);
        }
        String sanitized =
                candidate
                        .replaceAll("[\\\\/:*?\"<>|\\p{Cntrl}]", "_")
                        .replaceAll("\\s+", " ")
                        .trim();
        if (sanitized.isEmpty()) {
            sanitized = "attachment-" + index + extension(mimeType);
        }
        if (sanitized.length() > 100) {
            sanitized = sanitized.substring(0, 100);
        }
        return String.format(
                Locale.ROOT,
                "%020d-%02d-%s-%s",
                Math.max(0L, message.sequenceNumber()),
                index,
                sha256(bytes),
                sanitized);
    }

    private static String extension(String mimeType) {
        if (mimeType == null) {
            return ".bin";
        }
        switch (mimeType.toLowerCase(Locale.ROOT)) {
            case "image/png":
                return ".png";
            case "image/jpeg":
                return ".jpg";
            case "image/gif":
                return ".gif";
            case "application/pdf":
                return ".pdf";
            default:
                return ".bin";
        }
    }

    private static JsonNode replaceExactText(
            JsonNode node, String target, String replacement) {
        if (target == null || target.isEmpty() || node == null) {
            return node;
        }
        if (node.isTextual()) {
            String value = node.asText();
            return value.equals(target) ? TextNode.valueOf(replacement) : node;
        }
        if (node instanceof ObjectNode) {
            ObjectNode object = (ObjectNode) node;
            List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
            Iterator<Map.Entry<String, JsonNode>> iterator = object.fields();
            iterator.forEachRemaining(fields::add);
            for (Map.Entry<String, JsonNode> field : fields) {
                object.set(
                        field.getKey(),
                        replaceExactText(field.getValue(), target, replacement));
            }
        } else if (node instanceof ArrayNode) {
            ArrayNode array = (ArrayNode) node;
            for (int index = 0; index < array.size(); index++) {
                array.set(index, replaceExactText(array.get(index), target, replacement));
            }
        }
        return node;
    }

    private static JsonNode replacePathReference(
            JsonNode node, String target, String replacement) {
        if (target == null || target.isEmpty() || node == null) {
            return node;
        }
        if (node.isTextual()) {
            return node;
        }
        if (node instanceof ObjectNode) {
            ObjectNode object = (ObjectNode) node;
            List<Map.Entry<String, JsonNode>> fields = new ArrayList<>();
            object.fields().forEachRemaining(fields::add);
            for (Map.Entry<String, JsonNode> field : fields) {
                JsonNode value = field.getValue();
                if (value.isTextual()) {
                    if ("text".equals(field.getKey())) {
                        object.put(
                                field.getKey(),
                                replaceRecognizedPathWrappers(
                                        value.asText(), target, replacement));
                    } else if (isPathField(field.getKey()) && value.asText().equals(target)) {
                        object.put(field.getKey(), replacement);
                    }
                } else {
                    object.set(
                            field.getKey(),
                            replacePathReference(value, target, replacement));
                }
            }
        } else if (node instanceof ArrayNode) {
            ArrayNode array = (ArrayNode) node;
            for (int index = 0; index < array.size(); index++) {
                array.set(
                        index,
                        replacePathReference(array.get(index), target, replacement));
            }
        }
        return node;
    }

    private static boolean isPathField(String field) {
        return "path".equals(field)
                || "file".equals(field)
                || "filePath".equals(field)
                || "file_path".equals(field)
                || "saved_path".equals(field)
                || "local_path".equals(field)
                || "image_url".equals(field);
    }

    private static String replaceRecognizedPathWrappers(
            String value, String target, String replacement) {
        if (target == null || target.isEmpty()) {
            return value;
        }

        // Codex mentioned-file wrappers use one path as the complete value after "## name:".
        Pattern mentionedFile =
                Pattern.compile(
                        "(?m)^(##[ \\t]+.*?:[ \\t]*)"
                                + Pattern.quote(target)
                                + "([ \\t]*\\r?$)");
        value =
                mentionedFile
                        .matcher(value)
                        .replaceAll(
                                "$1" + Matcher.quoteReplacement(replacement) + "$2");

        // Older Codex image wrappers can carry a local path attribute.
        value =
                Pattern.compile(
                                "(?i)(\\bpath\\s*=\\s*\")"
                                        + Pattern.quote(target)
                                        + "(\")")
                        .matcher(value)
                        .replaceAll(
                                "$1" + Matcher.quoteReplacement(replacement) + "$2");
        value =
                Pattern.compile(
                                "(?i)(\\bpath\\s*=\\s*')"
                                        + Pattern.quote(target)
                                        + "(')")
                        .matcher(value)
                        .replaceAll(
                                "$1" + Matcher.quoteReplacement(replacement) + "$2");

        // Claude file mentions are emitted as @"path" (or occasionally single quoted).
        value =
                value.replace(
                                "@\"" + target + "\"",
                                "@\"" + replacement + "\"")
                        .replace("@'" + target + "'", "@'" + replacement + "'");
        return value;
    }

    private static String text(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value == null || value.isNull() ? null : value.asText();
    }

    private static void recordMissingAttachment(
            ArrayNode missingAttachments,
            JsonNode metadata,
            ChatMessage message,
            int index) {
        missingAttachments.add(metadata.deepCopy());
        LOG.warn(
                "Attachment {} is unavailable while restoring {} session {} message {}",
                index,
                message.sessionKey().sourceType(),
                message.sessionKey().sessionId(),
                message.messageId());
    }

    private static String sha256(byte[] bytes) {
        return hex(digest().digest(bytes));
    }

    private static String sha256(Path path) throws IOException {
        MessageDigest digest = digest();
        try (InputStream input = Files.newInputStream(path);
                DigestInputStream hashing = new DigestInputStream(input, digest)) {
            byte[] buffer = new byte[8192];
            while (hashing.read(buffer) >= 0) {
                // DigestInputStream updates the digest as bytes are consumed.
            }
        }
        return hex(digest.digest());
    }

    private static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private static String hex(byte[] bytes) {
        StringBuilder result = new StringBuilder(bytes.length * 2);
        for (byte value : bytes) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
