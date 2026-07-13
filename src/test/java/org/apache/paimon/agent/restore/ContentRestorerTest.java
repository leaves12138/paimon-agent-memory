package org.apache.paimon.agent.restore;

import org.apache.paimon.agent.model.AttachmentPayload;
import org.apache.paimon.agent.model.ChatMessage;
import org.apache.paimon.agent.model.SessionKey;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

class ContentRestorerTest {

    @TempDir Path tempDir;

    @Test
    void restoresInlineAndLocalAttachmentsAndRemovesInternalManifest() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String content =
                "{\"file\":\"/source/report.txt\","
                        + "\"image\":\"paimon-blob:1\","
                        + "\"note\":\"literal paimon-blob:1 inside text\","
                        + "\"unrelated\":\"prefix /source/report.txt suffix\","
                        + "\"_paimon_attachments\":["
                        + "{\"index\":0,\"source_kind\":\"local_path\","
                        + "\"source_reference\":\"/source/report.txt\",\"file_name\":\"report.txt\"},"
                        + "{\"index\":1,\"source_kind\":\"base64\","
                        + "\"source_reference\":\"inline:image/png\",\"mime_type\":\"image/png\"}]}";
        ChatMessage message =
                new ChatMessage(
                        "m1",
                        new SessionKey("claude", "s1"),
                        12L,
                        "user",
                        "user",
                        content,
                        Arrays.asList(
                                AttachmentPayload.of(
                                        "file body".getBytes(StandardCharsets.UTF_8)),
                                AttachmentPayload.of(new byte[] {1, 2, 3})),
                        Instant.EPOCH,
                        Instant.EPOCH);

        JsonNode restored =
                mapper.readTree(
                        new ContentRestorer(mapper)
                                .restore(message, tempDir.resolve("attachments")));

        assertThat(restored.has("_paimon_attachments")).isFalse();
        assertThat(restored.path("image").asText()).isEqualTo("AQID");
        assertThat(restored.path("note").asText())
                .isEqualTo("literal paimon-blob:1 inside text");
        assertThat(restored.path("unrelated").asText())
                .isEqualTo("prefix /source/report.txt suffix");
        Path restoredFile = Path.of(restored.path("file").asText());
        assertThat(restoredFile).isRegularFile();
        assertThat(Files.readString(restoredFile)).isEqualTo("file body");
        assertThat(restoredFile.getParent()).isEqualTo(tempDir.resolve("attachments"));
    }

    @Test
    void preservesAVisibleMarkerAndMetadataWhenInlineBytesAreMissing() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ChatMessage message =
                new ChatMessage(
                        "m2",
                        new SessionKey("codex", "s2"),
                        1L,
                        "user",
                        "response_item",
                        "{\"image\":\"paimon-blob:0\",\"_paimon_attachments\":["
                                + "{\"index\":0,\"source_kind\":\"data_uri\","
                                + "\"source_reference\":\"inline:image/png\","
                                + "\"mime_type\":\"image/png\",\"status\":\"too_large\"}]}",
                        Arrays.asList(AttachmentPayload.missing()),
                        Instant.EPOCH,
                        Instant.EPOCH);

        JsonNode restored =
                mapper.readTree(
                        new ContentRestorer(mapper)
                                .restore(message, tempDir.resolve("attachments")));

        assertThat(restored.path("image").asText()).isEqualTo("paimon-blob:0");
        assertThat(restored.path("_paimon_restore_missing_attachments")).hasSize(1);
        assertThat(restored.path("_paimon_restore_missing_attachments").get(0).path("status").asText())
                .isEqualTo("too_large");
    }

    @Test
    void replacesOnlyExactPathsAndRecognizedWrappersForShortRelativeNames() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        ChatMessage message =
                new ChatMessage(
                        "m3",
                        new SessionKey("claude", "s3"),
                        3L,
                        "user",
                        "user",
                        "{\"message\":{\"content\":["
                                + "{\"type\":\"text\",\"text\":\"a cat at a.txt\"},"
                                + "{\"type\":\"text\",\"text\":\"a\"},"
                                + "{\"type\":\"text\",\"text\":\"@\\\"a\\\"\"},"
                                + "{\"type\":\"text\",\"text\":\"## file: a\"},"
                                + "{\"type\":\"text\",\"text\":\"<image path=\\\"a\\\">\"},"
                                + "{\"type\":\"file\",\"path\":\"a\"}]},"
                                + "\"_paimon_attachments\":[{\"index\":0,"
                                + "\"source_kind\":\"local_path\","
                                + "\"source_reference\":\"a\",\"file_name\":\"a\"}]}",
                        Arrays.asList(AttachmentPayload.of(new byte[] {7})),
                        Instant.EPOCH,
                        Instant.EPOCH);

        JsonNode restored =
                mapper.readTree(
                        new ContentRestorer(mapper)
                                .restore(message, tempDir.resolve("short-path-attachments")));

        JsonNode content = restored.path("message").path("content");
        String restoredPath = content.get(5).path("path").asText();
        assertThat(content.get(0).path("text").asText()).isEqualTo("a cat at a.txt");
        assertThat(content.get(1).path("text").asText()).isEqualTo("a");
        assertThat(content.get(2).path("text").asText())
                .isEqualTo("@\"" + restoredPath + "\"");
        assertThat(content.get(3).path("text").asText())
                .isEqualTo("## file: " + restoredPath);
        assertThat(content.get(4).path("text").asText())
                .isEqualTo("<image path=\"" + restoredPath + "\">");
        assertThat(Path.of(restoredPath)).isRegularFile();
    }
}
