package org.apache.paimon.agent.dashboard;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DashboardContentPreviewTest {

    @Test
    void extractsCodexUserAndAssistantText() {
        String user =
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                        + "\"role\":\"user\",\"content\":["
                        + "{\"type\":\"input_text\",\"text\":\"hello\\nworld\"},"
                        + "{\"type\":\"input_image\",\"image_url\":\"paimon-blob:0\"}]}}";
        String assistant =
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"message\","
                        + "\"role\":\"assistant\",\"content\":["
                        + "{\"type\":\"output_text\",\"text\":\"Here is the answer.\"}]}}";

        assertThat(DashboardContentPreview.preview(user)).isEqualTo("hello\nworld · 图片");
        assertThat(DashboardContentPreview.preview(assistant)).isEqualTo("Here is the answer.");
    }

    @Test
    void summarizesCodexFunctionCallAndOutput() {
        String call =
                "{\"type\":\"response_item\",\"payload\":{\"type\":\"function_call\","
                        + "\"name\":\"read_file\",\"arguments\":\"{\\\"path\\\":\\\"README.md\\\"}\"}}";
        String output =
                "{\"type\":\"response_item\",\"payload\":{"
                        + "\"type\":\"function_call_output\",\"output\":\"file contents\"}}";

        assertThat(DashboardContentPreview.preview(call))
                .isEqualTo("调用工具 read_file: {\"path\":\"README.md\"}");
        assertThat(DashboardContentPreview.preview(output)).isEqualTo("工具返回: file contents");
    }

    @Test
    void summarizesStructuredCodexToolParameters() {
        String webSearch =
                "{\"type\":\"response_item\",\"payload\":{"
                        + "\"type\":\"web_search_call\",\"action\":{"
                        + "\"type\":\"search\",\"query\":\"paimon blob\","
                        + "\"url\":\"https://example.test/result\"}}}";
        String localShell =
                "{\"type\":\"response_item\",\"payload\":{"
                        + "\"type\":\"local_shell_call\",\"action\":{"
                        + "\"type\":\"exec\",\"command\":[\"git\",\"status\"],"
                        + "\"path\":\"/tmp/project\"}}}";
        String structuredArguments =
                "{\"type\":\"response_item\",\"payload\":{"
                        + "\"type\":\"mcp_tool_call\",\"name\":\"lookup\","
                        + "\"arguments\":{\"path\":\"README.md\",\"query\":\"blob\"}}}";
        String customInput =
                "{\"type\":\"response_item\",\"payload\":{"
                        + "\"type\":\"custom_tool_call\",\"name\":\"shell\","
                        + "\"input\":\"echo hello\"}}";

        assertThat(DashboardContentPreview.preview(webSearch))
                .isEqualTo(
                        "调用工具 web_search: query=paimon blob · url=https://example.test/result");
        assertThat(DashboardContentPreview.preview(localShell))
                .isEqualTo(
                        "调用工具 local_shell: command=git · command=status · path=/tmp/project");
        assertThat(DashboardContentPreview.preview(structuredArguments))
                .isEqualTo("调用工具 lookup: path=README.md · query=blob");
        assertThat(DashboardContentPreview.preview(customInput))
                .isEqualTo("调用工具 shell: echo hello");
    }

    @Test
    void extractsClaudeTextAndToolBlocks() {
        String text =
                "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\","
                        + "\"content\":[{\"type\":\"text\",\"text\":\"first line\\nsecond line\"}]}}";
        String tool =
                "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\","
                        + "\"content\":[{\"type\":\"tool_use\",\"name\":\"Bash\","
                        + "\"input\":{\"command\":\"pwd\"}}]}}";

        assertThat(DashboardContentPreview.preview(text)).isEqualTo("first line\nsecond line");
        assertThat(DashboardContentPreview.preview(tool))
                .isEqualTo("调用工具 Bash: command=pwd");
    }

    @Test
    void preservesMixedClaudeBlocksAndIgnoresToolInputShapeFields() {
        String mixed =
                "{\"type\":\"assistant\",\"message\":{\"role\":\"assistant\","
                        + "\"content\":["
                        + "{\"type\":\"text\",\"text\":\"我来检查\"},"
                        + "{\"type\":\"tool_use\",\"name\":\"Bash\",\"input\":{"
                        + "\"command\":\"pwd\",\"type\":\"tool_result\","
                        + "\"content\":\"this is an argument, not a result\"}}]}}";

        assertThat(DashboardContentPreview.preview(mixed))
                .isEqualTo("我来检查 · 调用工具 Bash: command=pwd");
    }

    @Test
    void summarizesGeneratedImagesAttachmentsDocumentsAndToolResults() {
        String generated =
                "{\"type\":\"event_msg\",\"payload\":{"
                        + "\"type\":\"image_generation_end\",\"result\":\"paimon-blob:0\"}}";
        String attachment =
                "{\"type\":\"attachment\",\"message\":{\"content\":["
                        + "{\"type\":\"document\",\"title\":\"spec\"},"
                        + "{\"type\":\"file\",\"name\":\"notes.txt\"}]}}";
        String toolResult =
                "{\"type\":\"user\",\"message\":{\"content\":["
                        + "{\"type\":\"tool_result\",\"content\":\"command finished\"}]}}";

        assertThat(DashboardContentPreview.preview(generated)).isEqualTo("生成图片");
        assertThat(DashboardContentPreview.preview(attachment)).isEqualTo("文档 · 附件");
        assertThat(DashboardContentPreview.preview(toolResult))
                .isEqualTo("工具返回: command finished");
    }

    @Test
    void onlyLabelsNonEmptyAttachmentManifests() {
        String withoutAttachment =
                "{\"type\":\"assistant\",\"message\":{\"content\":["
                        + "{\"type\":\"text\",\"text\":\"plain text\"}]},"
                        + "\"_paimon_attachments\":[]}";
        String withAttachment =
                "{\"type\":\"assistant\",\"message\":{\"content\":["
                        + "{\"type\":\"text\",\"text\":\"see image\"}]},"
                        + "\"_paimon_attachments\":[{\"index\":0}]}";

        assertThat(DashboardContentPreview.preview(withoutAttachment)).isEqualTo("plain text");
        assertThat(DashboardContentPreview.preview(withAttachment))
                .isEqualTo("see image · 附件");
    }

    @Test
    void boundsManyShortNestedToolValues() {
        StringBuilder blocks = new StringBuilder();
        for (int index = 0; index < 5_000; index++) {
            if (index > 0) {
                blocks.append(',');
            }
            blocks.append("{\"type\":\"text\",\"text\":\"value-")
                    .append(index)
                    .append("\"}");
        }
        String toolResult =
                "{\"type\":\"user\",\"message\":{\"content\":[{"
                        + "\"type\":\"tool_result\",\"content\":["
                        + blocks
                        + "]}]}}";

        String preview = DashboardContentPreview.preview(toolResult);

        assertThat(preview).startsWith("工具返回: value-0");
        assertThat(preview.codePointCount(0, preview.length())).isLessThanOrEqualTo(240);
    }

    @Test
    void handlesNullEmptyAndUnicodeTruncation() {
        assertThat(DashboardContentPreview.preview(null)).isEmpty();
        assertThat(DashboardContentPreview.preview("")).isEmpty();

        String emoji = "🙂".repeat(300);
        String json =
                "{\"type\":\"assistant\",\"message\":{\"content\":["
                        + "{\"type\":\"text\",\"text\":\""
                        + emoji
                        + "\"}]}}";
        String preview = DashboardContentPreview.preview(json);

        assertThat(preview).endsWith("…");
        assertThat(preview.codePointCount(0, preview.length())).isEqualTo(240);

        String largeOutput =
                "{\"type\":\"response_item\",\"payload\":{"
                        + "\"type\":\"function_call_output\",\"output\":\""
                        + "x".repeat(100_000)
                        + "\"}}";
        String outputPreview = DashboardContentPreview.preview(largeOutput);
        assertThat(outputPreview).startsWith("工具返回: ").endsWith("…");
        assertThat(outputPreview.codePointCount(0, outputPreview.length())).isEqualTo(240);

        String outputBeforeType =
                "{\"type\":\"response_item\",\"payload\":{\"output\":\""
                        + "y".repeat(100_000)
                        + "\",\"type\":\"function_call_output\"}}";
        String reorderedPreview = DashboardContentPreview.preview(outputBeforeType);
        assertThat(reorderedPreview).startsWith("工具返回: ").endsWith("…");
        assertThat(reorderedPreview.codePointCount(0, reorderedPreview.length()))
                .isEqualTo(240);
    }

    @Test
    void decodesEscapedQuotesBackslashesAndUnicodeSurrogates() {
        String json =
                "{\"type\":\"assistant\",\"message\":{\"content\":[{"
                        + "\"type\":\"text\","
                        + "\"text\":\"quote: \\\" slash: \\\\ smile: \\uD83D\\uDE42\"}]}}";

        assertThat(DashboardContentPreview.preview(json))
                .isEqualTo("quote: \" slash: \\ smile: 🙂");
    }

    @Test
    void preservesMarkdownParagraphListAndFenceLineBreaks() {
        String json =
                "{\"type\":\"assistant\",\"message\":{\"content\":[{"
                        + "\"type\":\"text\",\"text\":\"# Result\\r\\n\\r\\n"
                        + "- first\\n- second\\n\\n```java\\nint answer = 42;\\n```\"}]}}";

        assertThat(DashboardContentPreview.previewMessage(json, "assistant", "message"))
                .isEqualTo(
                        "# Result\n\n- first\n- second\n\n```java\nint answer = 42;\n```");
    }

    @Test
    void fallsBackForMalformedJsonAndNeverExceedsLimit() {
        String malformed = "  {broken   " + "x".repeat(300) + "  ";

        String preview = DashboardContentPreview.preview(malformed);

        assertThat(preview).startsWith("{broken xx").endsWith("…");
        assertThat(preview.codePointCount(0, preview.length())).isEqualTo(240);
    }

    @Test
    void supportsLongConversationPreviewsWhileKeepingToolEventsShort() {
        String json =
                "{\"type\":\"assistant\",\"message\":{\"content\":[{"
                        + "\"type\":\"text\",\"text\":\""
                        + "x".repeat(5_000)
                        + "\"}]}}";

        String normal = DashboardContentPreview.preview(json);
        String explicit = DashboardContentPreview.preview(json, 4_096);
        String user = DashboardContentPreview.previewMessage(json, "user", "message");
        String claudeAssistant =
                DashboardContentPreview.previewMessage(json, "assistant", "assistant");
        String assistant =
                DashboardContentPreview.previewMessage(
                        json, "assistant", "assistant_message");
        String tool =
                DashboardContentPreview.previewMessage(
                        json, "assistant", "custom_tool_call");

        assertThat(normal.codePointCount(0, normal.length())).isEqualTo(240);
        assertThat(explicit).endsWith("…");
        assertThat(explicit.codePointCount(0, explicit.length())).isEqualTo(4_096);
        assertThat(user.codePointCount(0, user.length())).isEqualTo(4_096);
        assertThat(claudeAssistant.codePointCount(0, claudeAssistant.length()))
                .isEqualTo(4_096);
        assertThat(assistant.codePointCount(0, assistant.length())).isEqualTo(4_096);
        assertThat(tool.codePointCount(0, tool.length())).isEqualTo(240);
        assertThat(DashboardContentPreview.messageLimit("tool", "message")).isEqualTo(240);
        assertThatThrownBy(() -> DashboardContentPreview.preview(json, 0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
