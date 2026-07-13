package org.apache.paimon.agent.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;

class AttachmentResolverTest {

    @Test
    void preservesAttachmentIndexesIncludingMissingElements() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AttachmentResolver resolver = new AttachmentResolver(mapper, true, false, 1024);
        String data = Base64.getEncoder().encodeToString("image".getBytes(StandardCharsets.UTF_8));

        ResolvedAttachments resolved =
                resolver.resolve(
                        mapper.readTree("{\"type\":\"user\"}"),
                        Arrays.asList(
                                new AttachmentReference(
                                        AttachmentReference.Kind.DATA_URI,
                                        "data:image/png;base64," + data,
                                        "a.png",
                                        "image/png",
                                        null),
                                new AttachmentReference(
                                        AttachmentReference.Kind.REMOTE_URL,
                                        "https://example.invalid/image.png",
                                        "remote.png",
                                        "image/png",
                                        null)));

        assertThat(resolved.payloads()).hasSize(2);
        assertThat(resolved.payloads().get(0).bytes())
                .isEqualTo("image".getBytes(StandardCharsets.UTF_8));
        assertThat(resolved.payloads().get(1).isMissing()).isTrue();
        JsonNode json = mapper.readTree(resolved.contentJson());
        assertThat(json.path("_paimon_attachments").get(0).path("status").asText())
                .isEqualTo("stored");
        assertThat(json.path("_paimon_attachments").get(1).path("status").asText())
                .isEqualTo("remote_not_downloaded");
    }

    @Test
    void rejectsBase64PayloadBeforeItCanExceedTheConfiguredLimit() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AttachmentResolver resolver = new AttachmentResolver(mapper, true, false, 3);
        String encoded =
                Base64.getEncoder().encodeToString("four".getBytes(StandardCharsets.UTF_8));

        ResolvedAttachments resolved =
                resolver.resolve(
                        mapper.createObjectNode(),
                        Arrays.asList(
                                new AttachmentReference(
                                        AttachmentReference.Kind.BASE64,
                                        encoded,
                                        "oversized.txt",
                                        "text/plain",
                                        null)));

        assertThat(resolved.payloads().get(0).isMissing()).isTrue();
        JsonNode manifest =
                mapper.readTree(resolved.contentJson()).path("_paimon_attachments").get(0);
        assertThat(manifest.path("status").asText()).isEqualTo("too_large");
        assertThat(manifest.path("size").asLong()).isEqualTo(4L);
    }

    @Test
    void percentDecodesPlainDataUriWithoutTreatingPlusAsSpace() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AttachmentResolver resolver = new AttachmentResolver(mapper, true, false, 1024);

        ResolvedAttachments resolved =
                resolver.resolve(
                        mapper.createObjectNode(),
                        Arrays.asList(
                                new AttachmentReference(
                                        AttachmentReference.Kind.DATA_URI,
                                        "data:text/plain,a%20b+c%2B%E4%B8%96%E7%95%8C",
                                        "plain.txt",
                                        "text/plain",
                                        null)));

        assertThat(resolved.payloads().get(0).bytes())
                .isEqualTo("a b+c+世界".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void redactsRemoteCredentialsQueryAndFragmentFromManifest() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        AttachmentResolver resolver = new AttachmentResolver(mapper, true, false, 1024);

        ResolvedAttachments resolved =
                resolver.resolve(
                        mapper.createObjectNode(),
                        Arrays.asList(
                                new AttachmentReference(
                                        AttachmentReference.Kind.REMOTE_URL,
                                        "https://user:password@example.invalid/image.png?token=secret#fragment",
                                        "remote.png",
                                        "image/png",
                                        null)));

        JsonNode manifest =
                mapper.readTree(resolved.contentJson()).path("_paimon_attachments").get(0);
        assertThat(manifest.path("source_reference").asText())
                .isEqualTo("https://example.invalid/image.png");
        assertThat(resolved.contentJson())
                .doesNotContain("password", "token", "secret", "fragment");
    }
}
