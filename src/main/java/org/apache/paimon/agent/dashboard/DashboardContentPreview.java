package org.apache.paimon.agent.dashboard;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Builds a bounded preview while preserving transcript structure and avoiding JSON trees. */
final class DashboardContentPreview {

    static final int DEFAULT_MAX_LENGTH = 240;
    static final int CONVERSATION_MAX_LENGTH = 4096;
    private static final int TYPE_LIMIT = 80;
    private static final int DETAIL_LIMIT = 180;
    private static final int MAX_PARTS = 32;
    private static final String CODEX_FILES_MARKER = "# Files mentioned by the user:";
    private static final String CODEX_REQUEST_MARKER = "## My request for Codex:";
    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    private DashboardContentPreview() {}

    static String preview(String contentJson) {
        return preview(contentJson, DEFAULT_MAX_LENGTH);
    }

    static String preview(String contentJson, int maxChars) {
        if (maxChars <= 0) {
            throw new IllegalArgumentException("maxChars must be greater than zero");
        }
        if (contentJson == null || contentJson.isEmpty()) {
            return "";
        }

        try (JsonParser parser = JSON_FACTORY.createParser(contentJson)) {
            ParsedPreview parsed = parse(contentJson, parser, maxChars, false, false);
            String value = parsed.render();
            return isBlank(value)
                    ? compactRaw(contentJson, maxChars)
                    : normalizeAndTruncate(value, maxChars);
        } catch (IOException | RuntimeException ignored) {
            // Partial and future transcript shapes remain browsable as bounded raw text.
            return compactRaw(contentJson, maxChars);
        }
    }

    static String previewMessage(String contentJson, String role, String eventType) {
        int maxChars = messageLimit(role, eventType);
        if (!isUserRole(role)) {
            return preview(contentJson, maxChars);
        }
        if (contentJson == null || contentJson.isEmpty()) {
            return "";
        }
        try (JsonParser parser = JSON_FACTORY.createParser(contentJson)) {
            ParsedPreview parsed = parse(contentJson, parser, maxChars, false, true);
            String value = parsed.render();
            if (isBlank(value)) {
                return parsed.isCodexUserEnvelope() ? "" : compactRaw(contentJson, maxChars);
            }
            return normalizeAndTruncate(value, maxChars);
        } catch (IOException | RuntimeException ignored) {
            return compactRaw(contentJson, maxChars);
        }
    }

    static String conversationPreviewMessage(
            String contentJson, String role, String eventType) {
        int maxChars = messageLimit(role, eventType);
        if (contentJson == null || contentJson.isEmpty()) {
            return "";
        }
        try (JsonParser parser = JSON_FACTORY.createParser(contentJson)) {
            String value =
                    parse(contentJson, parser, maxChars, true, isUserRole(role))
                            .renderConversation();
            return isBlank(value) ? "" : normalizeAndTruncate(value, maxChars);
        } catch (IOException | RuntimeException ignored) {
            // Unknown or partially written message shapes are preserved instead of silently
            // disappearing from the conversation. Known tool blocks are filtered structurally.
            return compactRaw(contentJson, maxChars);
        }
    }

    static int messageLimit(String role, String eventType) {
        String normalizedRole = role == null ? "" : role.toLowerCase(java.util.Locale.ROOT);
        String normalizedEvent =
                eventType == null ? "" : eventType.toLowerCase(java.util.Locale.ROOT);
        if ("tool".equals(normalizedRole)
                || normalizedEvent.contains("tool")
                || normalizedEvent.contains("call")
                || normalizedEvent.endsWith("_output")) {
            return DEFAULT_MAX_LENGTH;
        }
        if ("user".equals(normalizedRole) || "assistant".equals(normalizedRole)) {
            return CONVERSATION_MAX_LENGTH;
        }
        return DEFAULT_MAX_LENGTH;
    }

    private static boolean isUserRole(String role) {
        return role != null && "user".equalsIgnoreCase(role);
    }

    private static ParsedPreview parse(
            String source,
            JsonParser parser,
            int maxChars,
            boolean conversationOnly,
            boolean cleanCodexUserEnvelope)
            throws IOException {
        ParsedPreview result = new ParsedPreview(maxChars, cleanCodexUserEnvelope);
        Context context = null;
        ContentBlock block = null;
        Payload payload = null;

        while (true) {
            JsonToken token = parser.nextToken();
            if (token == null) {
                break;
            }
            if (token == JsonToken.START_OBJECT || token == JsonToken.START_ARRAY) {
                String parentField = context == null ? null : context.currentField;
                context =
                        new Context(
                                token == JsonToken.START_OBJECT,
                                context,
                                parentField);
                if ((context.object && isRootAttachmentField(context))
                        || (context.parent != null && isAttachmentManifest(context.parent))) {
                    result.hasAttachments = true;
                }
                if (isPayload(context)) {
                    payload = new Payload(context, maxChars, cleanCodexUserEnvelope);
                } else if (isContentItem(context)) {
                    block = new ContentBlock(context, maxChars, cleanCodexUserEnvelope);
                }
                continue;
            }
            if (token == JsonToken.END_OBJECT || token == JsonToken.END_ARRAY) {
                if (block != null && context == block.context) {
                    result.add(block);
                    block = null;
                }
                if (payload != null && context == payload.context) {
                    result.add(payload);
                    payload = null;
                }
                context = context == null ? null : context.parent;
                if (result.isFull(conversationOnly)) {
                    break;
                }
                continue;
            }
            if (token == JsonToken.FIELD_NAME) {
                if (context != null) {
                    context.currentField = parser.currentName();
                }
                continue;
            }
            if (context != null
                    && isAttachmentManifest(context)
                    && token != JsonToken.VALUE_NULL) {
                result.hasAttachments = true;
            }
            if (token != JsonToken.VALUE_STRING || context == null) {
                continue;
            }

            String field = context.currentField;
            if (isRoot(context)) {
                if ("type".equals(field)) {
                    result.rootType = stringValue(source, parser, TYPE_LIMIT).text;
                } else if ("message".equals(field)
                        || "text".equals(field)
                        || "content".equals(field)
                        || "output".equals(field)) {
                    Snippet text =
                            userTextValue(
                                    source, parser, maxChars, cleanCodexUserEnvelope);
                    result.addText(text);
                    if (text.truncated) {
                        break;
                    }
                }
                continue;
            }

            if (payload != null && context == payload.context) {
                Snippet snippet = payload.accept(field, source, parser);
                if (snippet != null && payload.shouldStop(snippet) && payload.canRender()) {
                    boolean tool = payload.isTool();
                    result.add(payload);
                    payload = null;
                    if (!conversationOnly || !tool) {
                        break;
                    }
                }
                continue;
            }

            if (block != null && isDescendant(context, block.context)) {
                Snippet snippet = block.accept(context, field, source, parser);
                if (snippet != null && block.shouldStop(snippet) && block.canRender()) {
                    boolean tool = block.isTool();
                    result.add(block);
                    block = null;
                    if (!conversationOnly || !tool) {
                        break;
                    }
                }
                continue;
            }

            if (payload != null && isStructuredPayloadValue(context, payload.context)) {
                String detailField = structuredField(context);
                Snippet snippet = payload.acceptStructured(detailField, source, parser);
                if (snippet != null && payload.shouldStop(snippet) && payload.canRender()) {
                    boolean tool = payload.isTool();
                    result.add(payload);
                    payload = null;
                    if (!conversationOnly || !tool) {
                        break;
                    }
                }
                continue;
            }

            if (isMessage(context) && "content".equals(field)) {
                Snippet text =
                        userTextValue(source, parser, maxChars, cleanCodexUserEnvelope);
                result.addText(text);
                if (text.truncated) {
                    break;
                }
            } else if (isDirectContentArray(context)) {
                Snippet text =
                        userTextValue(source, parser, maxChars, cleanCodexUserEnvelope);
                result.addText(text);
                if (text.truncated) {
                    break;
                }
            }
        }
        return result;
    }

    private static boolean isRoot(Context context) {
        return context.object && context.parent == null;
    }

    private static boolean isPayload(Context context) {
        return context.object
                && context.parent != null
                && isRoot(context.parent)
                && "payload".equals(context.parentField);
    }

    private static boolean isMessage(Context context) {
        return context.object
                && context.parent != null
                && isRoot(context.parent)
                && "message".equals(context.parentField);
    }

    private static boolean isContentItem(Context context) {
        if (!context.object || context.parent == null || context.parent.object) {
            return false;
        }
        Context array = context.parent;
        return "content".equals(array.parentField)
                && array.parent != null
                && (isPayload(array.parent) || isMessage(array.parent));
    }

    private static boolean isDirectContentArray(Context context) {
        return !context.object
                && "content".equals(context.parentField)
                && context.parent != null
                && (isPayload(context.parent) || isMessage(context.parent));
    }

    private static boolean isRootAttachmentField(Context context) {
        return context.parent != null
                && isRoot(context.parent)
                && "_paimon_attachments".equals(context.parentField);
    }

    private static boolean isAttachmentManifest(Context context) {
        return !context.object && isRootAttachmentField(context);
    }

    private static boolean isDescendant(Context context, Context ancestor) {
        Context current = context;
        while (current != null) {
            if (current == ancestor) {
                return true;
            }
            current = current.parent;
        }
        return false;
    }

    private static boolean isStructuredPayloadValue(Context context, Context payloadContext) {
        if (context == null || context == payloadContext) {
            return false;
        }
        Context child = context;
        while (child.parent != null && child.parent != payloadContext) {
            child = child.parent;
        }
        if (child.parent != payloadContext) {
            return false;
        }
        return "action".equals(child.parentField)
                || "arguments".equals(child.parentField)
                || "input".equals(child.parentField)
                || "command".equals(child.parentField);
    }

    private static String structuredField(Context context) {
        if (context.currentField != null) {
            return context.currentField;
        }
        return context.object ? null : context.parentField;
    }

    private static boolean isDetailField(String field) {
        return "query".equals(field)
                || "queries".equals(field)
                || "url".equals(field)
                || "command".equals(field)
                || "path".equals(field)
                || "file_path".equals(field);
    }

    private static Snippet stringValue(String source, JsonParser parser, int limit)
            throws IOException {
        long offset = parser.currentTokenLocation().getCharOffset();
        if (offset >= 0 && offset < source.length()) {
            int start = (int) offset;
            while (start < source.length() && source.charAt(start) != '"') {
                start++;
            }
            if (start < source.length()) {
                return decodeString(source, start + 1, limit);
            }
        }
        return bounded(parser.getValueAsString(), limit);
    }

    private static Snippet userTextValue(
            String source, JsonParser parser, int limit, boolean cleanCodexUserEnvelope)
            throws IOException {
        if (!cleanCodexUserEnvelope) {
            return stringValue(source, parser, limit);
        }
        long offset = parser.currentTokenLocation().getCharOffset();
        if (offset < 0 || offset >= source.length()) {
            return stringValue(source, parser, limit);
        }
        int quote = (int) offset;
        while (quote < source.length() && source.charAt(quote) != '"') {
            quote++;
        }
        if (quote >= source.length()) {
            return stringValue(source, parser, limit);
        }
        int stringStart = quote + 1;
        int stringEnd = jsonStringEnd(source, stringStart);
        if (stringEnd < 0) {
            return stringValue(source, parser, limit);
        }
        int files =
                headingIndex(
                        source,
                        CODEX_FILES_MARKER,
                        stringStart,
                        stringEnd,
                        stringStart);
        if (files < 0) {
            return stringValue(source, parser, limit);
        }
        int request =
                headingIndex(
                        source,
                        CODEX_REQUEST_MARKER,
                        files + CODEX_FILES_MARKER.length(),
                        stringEnd,
                        stringStart);
        if (request < 0) {
            return stringValue(source, parser, limit);
        }
        if (!hasFileEntry(
                source,
                files + CODEX_FILES_MARKER.length(),
                request)) {
            return stringValue(source, parser, limit);
        }
        Snippet value = decodeString(source, request + CODEX_REQUEST_MARKER.length(), limit);
        return new Snippet(value.text, value.truncated, value.limit, true);
    }

    private static boolean hasFileEntry(String source, int start, int end) {
        int lineStart = start;
        while (lineStart < end) {
            int lineBreak = encodedLineBreak(source, lineStart, end);
            int lineEnd = lineBreak < 0 ? end : lineBreak;
            if (isFileEntryLine(source, lineStart, lineEnd)) {
                return true;
            }
            if (lineBreak < 0) {
                break;
            }
            lineStart = lineBreak + 2;
        }
        return false;
    }

    private static int encodedLineBreak(String source, int start, int end) {
        int index = start;
        while (index < end) {
            if (source.charAt(index) != '\\') {
                index++;
                continue;
            }
            int runStart = index;
            while (index < end && source.charAt(index) == '\\') {
                index++;
            }
            int count = index - runStart;
            if ((count & 1) == 1
                    && index < end
                    && (source.charAt(index) == 'n' || source.charAt(index) == 'r')) {
                return index - 1;
            }
            if (index < end) {
                index++;
            }
        }
        return -1;
    }

    private static boolean isFileEntryLine(String source, int start, int end) {
        if (start + 3 >= end || !source.startsWith("## ", start)) {
            return false;
        }
        int separator = source.indexOf(": ", start + 3);
        while (separator >= 0 && separator + 2 < end) {
            if (separator < end
                    && hasNonWhitespace(source, start + 3, separator)
                    && isAbsolutePath(source, separator + 2, end)) {
                return true;
            }
            separator = source.indexOf(": ", separator + 2);
        }
        return false;
    }

    private static boolean hasNonWhitespace(String source, int start, int end) {
        for (int index = start; index < end; index++) {
            if (!Character.isWhitespace(source.charAt(index))) {
                return true;
            }
        }
        return false;
    }

    private static boolean isAbsolutePath(String source, int start, int end) {
        if (start >= end) {
            return false;
        }
        if (source.charAt(start) == '/') {
            return hasNonWhitespace(source, start + 1, end);
        }
        if (start + 1 < end
                && source.charAt(start) == '\\'
                && source.charAt(start + 1) == '/') {
            return hasNonWhitespace(source, start + 2, end);
        }
        if (start + 3 < end
                && isAsciiLetter(source.charAt(start))
                && source.charAt(start + 1) == ':'
                && (source.charAt(start + 2) == '/'
                        || (source.charAt(start + 2) == '\\'
                                && source.charAt(start + 3) == '\\'))) {
            int pathStart = source.charAt(start + 2) == '/' ? start + 3 : start + 4;
            return hasNonWhitespace(source, pathStart, end);
        }
        return start + 4 < end
                && source.charAt(start) == '\\'
                && source.charAt(start + 1) == '\\'
                && source.charAt(start + 2) == '\\'
                && source.charAt(start + 3) == '\\'
                && hasNonWhitespace(source, start + 4, end);
    }

    private static boolean isAsciiLetter(char value) {
        return (value >= 'A' && value <= 'Z') || (value >= 'a' && value <= 'z');
    }

    private static int jsonStringEnd(String source, int start) {
        int index = start;
        while (index < source.length()) {
            char current = source.charAt(index++);
            if (current == '\\') {
                if (index < source.length()) {
                    index++;
                }
            } else if (current == '"') {
                return index - 1;
            }
        }
        return -1;
    }

    private static int headingIndex(
            String source, String heading, int from, int stringEnd, int stringStart) {
        int index = source.indexOf(heading, from);
        while (index >= 0 && index + heading.length() <= stringEnd) {
            if (isEncodedLineStart(source, index, stringStart)
                    && isEncodedLineEnd(source, index + heading.length(), stringEnd)) {
                return index;
            }
            index = source.indexOf(heading, index + 1);
        }
        return -1;
    }

    private static boolean isEncodedLineStart(String source, int index, int stringStart) {
        if (index == stringStart) {
            return true;
        }
        if (index < stringStart + 2) {
            return false;
        }
        char escaped = source.charAt(index - 1);
        if (escaped != 'n' && escaped != 'r') {
            return false;
        }
        int backslashes = 0;
        int cursor = index - 2;
        while (cursor >= stringStart && source.charAt(cursor) == '\\') {
            backslashes++;
            cursor--;
        }
        return (backslashes & 1) == 1;
    }

    private static boolean isEncodedLineEnd(String source, int index, int stringEnd) {
        return index == stringEnd
                || (index + 1 < stringEnd
                        && source.charAt(index) == '\\'
                        && (source.charAt(index + 1) == 'n'
                                || source.charAt(index + 1) == 'r'));
    }

    private static boolean isSingleImageMarkup(String value) {
        if (value == null) {
            return false;
        }
        String trimmed = value.trim();
        if ("</image>".equals(trimmed)) {
            return true;
        }
        return trimmed.endsWith(">")
                && trimmed.startsWith("<image")
                && (trimmed.length() == "<image>".length()
                        || Character.isWhitespace(trimmed.charAt("<image".length())));
    }

    private static Snippet decodeString(String source, int index, int limit) {
        NormalizedBuilder value = new NormalizedBuilder(limit);
        boolean closed = false;
        while (index < source.length()) {
            char current = source.charAt(index++);
            if (current == '"') {
                closed = true;
                break;
            }
            int codePoint;
            if (current != '\\') {
                if (Character.isHighSurrogate(current)
                        && index < source.length()
                        && Character.isLowSurrogate(source.charAt(index))) {
                    codePoint = Character.toCodePoint(current, source.charAt(index++));
                } else {
                    codePoint = current;
                }
            } else {
                if (index >= source.length()) {
                    break;
                }
                char escaped = source.charAt(index++);
                switch (escaped) {
                    case 'b':
                        codePoint = '\b';
                        break;
                    case 'f':
                        codePoint = '\f';
                        break;
                    case 'n':
                        codePoint = '\n';
                        break;
                    case 'r':
                        codePoint = '\r';
                        break;
                    case 't':
                        codePoint = '\t';
                        break;
                    case 'u':
                        int first = unicodeEscape(source, index);
                        if (first < 0) {
                            return new Snippet(value.text(), true, limit);
                        }
                        index += 4;
                        if (Character.isHighSurrogate((char) first)
                                && index + 5 < source.length()
                                && source.charAt(index) == '\\'
                                && source.charAt(index + 1) == 'u') {
                            int second = unicodeEscape(source, index + 2);
                            if (second >= 0 && Character.isLowSurrogate((char) second)) {
                                codePoint =
                                        Character.toCodePoint((char) first, (char) second);
                                index += 6;
                                break;
                            }
                        }
                        codePoint = first;
                        break;
                    default:
                        codePoint = escaped;
                        break;
                }
            }
            if (!value.append(codePoint)) {
                return new Snippet(value.text(), true, limit);
            }
        }
        return new Snippet(value.text(), !closed, limit);
    }

    private static int unicodeEscape(String source, int index) {
        if (index + 4 > source.length()) {
            return -1;
        }
        int result = 0;
        for (int offset = 0; offset < 4; offset++) {
            int digit = Character.digit(source.charAt(index + offset), 16);
            if (digit < 0) {
                return -1;
            }
            result = (result << 4) | digit;
        }
        return result;
    }

    private static Snippet bounded(String value, int limit) {
        if (value == null) {
            return new Snippet("", false, limit);
        }
        NormalizedBuilder result = new NormalizedBuilder(limit);
        int index = 0;
        while (index < value.length()) {
            int codePoint = value.codePointAt(index);
            index += Character.charCount(codePoint);
            if (!result.append(codePoint)) {
                return new Snippet(result.text(), true, limit);
            }
        }
        return new Snippet(result.text(), false, limit);
    }

    private static String compactRaw(String value, int maxChars) {
        return bounded(value, maxChars).display();
    }

    private static String normalizeAndTruncate(String value, int maxChars) {
        return bounded(value, maxChars).display();
    }

    private static String firstNonBlank(String first, String second) {
        return isBlank(first) ? second : first;
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static final class Context {
        private final boolean object;
        private final Context parent;
        private final String parentField;
        private String currentField;

        private Context(boolean object, Context parent, String parentField) {
            this.object = object;
            this.parent = parent;
            this.parentField = parentField;
        }
    }

    private static final class Payload {
        private final Context context;
        private final int maxChars;
        private final boolean cleanCodexUserEnvelope;
        private final BoundedParts text;
        private final BoundedParts details;
        private String type;
        private String name;
        private String output;
        private boolean codexUserEnvelope;

        private Payload(Context context, int maxChars, boolean cleanCodexUserEnvelope) {
            this.context = context;
            this.maxChars = maxChars;
            this.cleanCodexUserEnvelope = cleanCodexUserEnvelope;
            this.text = new BoundedParts(maxChars);
            this.details = new BoundedParts(Math.min(DETAIL_LIMIT, maxChars));
        }

        private Snippet accept(String field, String source, JsonParser parser)
                throws IOException {
            if ("type".equals(field)) {
                Snippet value = stringValue(source, parser, TYPE_LIMIT);
                type = value.text;
                return value;
            }
            if ("name".equals(field) || "tool_name".equals(field)) {
                Snippet value = stringValue(source, parser, TYPE_LIMIT);
                name = firstNonBlank(name, value.text);
                return value;
            }
            if ("arguments".equals(field)
                    || "action".equals(field)
                    || "input".equals(field)
                    || "command".equals(field)) {
                Snippet value = stringValue(source, parser, DETAIL_LIMIT);
                details.add(value.display());
                return value;
            }
            if ("output".equals(field) || "result".equals(field)) {
                Snippet value = stringValue(source, parser, maxChars);
                output = firstNonBlank(output, value.display());
                return value;
            }
            if ("message".equals(field)
                    || "text".equals(field)
                    || "content".equals(field)) {
                Snippet value =
                        userTextValue(
                                source, parser, maxChars, cleanCodexUserEnvelope);
                codexUserEnvelope |= value.codexUserEnvelope;
                text.add(value.display());
                return value;
            }
            return null;
        }

        private Snippet acceptStructured(String field, String source, JsonParser parser)
                throws IOException {
            if (!isDetailField(field)) {
                return null;
            }
            Snippet value = stringValue(source, parser, DETAIL_LIMIT);
            details.add(field + "=" + value.display());
            return value;
        }

        private boolean shouldStop(Snippet value) {
            return value.truncated || text.isFull() || details.isFull();
        }

        private boolean canRender() {
            return !isBlank(type);
        }

        private boolean isTool() {
            return type != null
                    && (type.endsWith("_call") || type.endsWith("_call_output"));
        }

        private String render() {
            if ("image_generation_end".equals(type)) {
                return "生成图片";
            }
            if (type != null && type.endsWith("_call_output")) {
                String value = firstNonBlank(output, text.join());
                return isBlank(value) ? "工具返回" : "工具返回: " + value;
            }
            if (type != null && type.endsWith("_call")) {
                String tool = firstNonBlank(name, toolName(type));
                String label = isBlank(tool) ? "调用工具" : "调用工具 " + tool;
                String value = details.join();
                return isBlank(value) ? label : label + ": " + value;
            }
            return text.join();
        }

        private boolean describesAttachment() {
            return "image_generation_end".equals(type);
        }

        private boolean isCodexUserEnvelope() {
            return codexUserEnvelope;
        }
    }

    private static final class ContentBlock {
        private final Context context;
        private final int maxChars;
        private final boolean cleanCodexUserEnvelope;
        private final BoundedParts text;
        private final BoundedParts nestedOutput;
        private final BoundedParts detail;
        private String type;
        private String name;
        private boolean codexUserEnvelope;

        private ContentBlock(Context context, int maxChars, boolean cleanCodexUserEnvelope) {
            this.context = context;
            this.maxChars = maxChars;
            this.cleanCodexUserEnvelope = cleanCodexUserEnvelope;
            this.text = new BoundedParts(maxChars);
            this.nestedOutput = new BoundedParts(maxChars);
            this.detail = new BoundedParts(Math.min(DETAIL_LIMIT, maxChars));
        }

        private Snippet accept(
                Context valueContext, String field, String source, JsonParser parser)
                throws IOException {
            String nestedField = field == null ? structuredField(valueContext) : field;
            if (valueContext == context) {
                if ("type".equals(field)) {
                    Snippet value = stringValue(source, parser, TYPE_LIMIT);
                    type = value.text;
                    return value;
                }
                if ("name".equals(field) || "tool_name".equals(field)) {
                    Snippet value = stringValue(source, parser, TYPE_LIMIT);
                    name = firstNonBlank(name, value.text);
                    return value;
                }
                if ("text".equals(field)
                        || "refusal".equals(field)
                        || "content".equals(field)
                        || "output".equals(field)) {
                    Snippet value =
                            userTextValue(
                                    source, parser, maxChars, cleanCodexUserEnvelope);
                    codexUserEnvelope |= value.codexUserEnvelope;
                    text.add(value.display());
                    return value;
                }
            } else if ((isBlank(type) || isToolOutput())
                    && ("text".equals(nestedField)
                            || "content".equals(nestedField)
                            || "output".equals(nestedField)
                            || "result".equals(nestedField))) {
                Snippet value = stringValue(source, parser, maxChars);
                nestedOutput.add(value.display());
                return value;
            } else if ((isBlank(type) || isToolCall()) && isDetailField(nestedField)) {
                Snippet value = stringValue(source, parser, DETAIL_LIMIT);
                detail.add(nestedField + "=" + value.display());
                return value;
            }
            return null;
        }

        private boolean isToolCall() {
            return "tool_use".equals(type) || (type != null && type.endsWith("_call"));
        }

        private boolean isToolOutput() {
            return "tool_result".equals(type)
                    || (type != null && type.endsWith("_call_output"));
        }

        private boolean isTool() {
            return isToolCall() || isToolOutput();
        }

        private boolean shouldStop(Snippet value) {
            return value.truncated || text.isFull() || nestedOutput.isFull() || detail.isFull();
        }

        private boolean canRender() {
            return !isBlank(type);
        }

        private String render() {
            if (isToolCall()) {
                String tool = firstNonBlank(name, toolName(type));
                String label = isBlank(tool) ? "调用工具" : "调用工具 " + tool;
                String value = detail.join();
                return isBlank(value) ? label : label + ": " + value;
            }
            if (isToolOutput()) {
                String value = firstNonBlank(text.join(), nestedOutput.join());
                return isBlank(value) ? "工具返回" : "工具返回: " + value;
            }
            String value = text.join();
            if ("input_image".equals(type) || "image".equals(type)) {
                return isBlank(value) ? "图片" : value + " · 图片";
            }
            if ("document".equals(type)) {
                return isBlank(value) ? "文档" : value + " · 文档";
            }
            if ("file".equals(type) || "attachment".equals(type)) {
                return isBlank(value) ? "附件" : value + " · 附件";
            }
            return value;
        }

        private boolean describesAttachment() {
            return "input_image".equals(type)
                    || "image".equals(type)
                    || "document".equals(type)
                    || "file".equals(type)
                    || "attachment".equals(type);
        }

        private boolean isCodexUserEnvelope() {
            return codexUserEnvelope;
        }

        private boolean isCodexImagePart() {
            return "input_image".equals(type) || isSingleImageMarkup(text.join());
        }
    }

    private static final class ParsedPreview {
        private final boolean cleanCodexUserEnvelope;
        private final BoundedParts parts;
        private final BoundedParts conversationParts;
        private String rootType;
        private boolean hasAttachments;
        private boolean attachmentDescribed;
        private boolean conversationAttachmentDescribed;
        private boolean codexUserEnvelope;

        private ParsedPreview(int maxChars, boolean cleanCodexUserEnvelope) {
            this.cleanCodexUserEnvelope = cleanCodexUserEnvelope;
            this.parts = new BoundedParts(maxChars);
            this.conversationParts = new BoundedParts(maxChars);
        }

        private void addText(Snippet value) {
            codexUserEnvelope |= value.codexUserEnvelope;
            add(value.display(), false, false);
        }

        private void add(Payload payload) {
            codexUserEnvelope |= payload.isCodexUserEnvelope();
            add(payload.render(), payload.describesAttachment(), payload.isTool());
        }

        private void add(ContentBlock block) {
            codexUserEnvelope |= block.isCodexUserEnvelope();
            if (cleanCodexUserEnvelope && codexUserEnvelope && block.isCodexImagePart()) {
                return;
            }
            add(block.render(), block.describesAttachment(), block.isTool());
        }

        private void add(String value, boolean describesAttachment, boolean tool) {
            parts.add(value);
            attachmentDescribed |= describesAttachment;
            if (!tool) {
                conversationParts.add(value);
                conversationAttachmentDescribed |= describesAttachment;
            }
        }

        private boolean isFull(boolean conversationOnly) {
            return conversationOnly ? conversationParts.isFull() : parts.isFull();
        }

        private String render() {
            if ("attachment".equals(rootType) && parts.isEmpty()) {
                add("附件", true, false);
            }
            if (hasAttachments && !attachmentDescribed && !codexUserEnvelope) {
                add("附件", true, false);
            }
            return parts.join();
        }

        private String renderConversation() {
            if ("attachment".equals(rootType) && conversationParts.isEmpty()) {
                conversationParts.add("附件");
                conversationAttachmentDescribed = true;
            }
            if (hasAttachments
                    && !conversationAttachmentDescribed
                    && !conversationParts.isEmpty()
                    && !codexUserEnvelope) {
                conversationParts.add("附件");
            }
            return conversationParts.join();
        }

        private boolean isCodexUserEnvelope() {
            return codexUserEnvelope;
        }
    }

    private static final class Snippet {
        private final String text;
        private final boolean truncated;
        private final int limit;
        private final boolean codexUserEnvelope;

        private Snippet(String text, boolean truncated, int limit) {
            this(text, truncated, limit, false);
        }

        private Snippet(
                String text, boolean truncated, int limit, boolean codexUserEnvelope) {
            this.text = text;
            this.truncated = truncated;
            this.limit = limit;
            this.codexUserEnvelope = codexUserEnvelope;
        }

        private String display() {
            if (!truncated || text.endsWith("…")) {
                return text;
            }
            int count = text.codePointCount(0, text.length());
            int keep = Math.min(count, Math.max(0, limit - 1));
            return text.substring(0, text.offsetByCodePoints(0, keep)) + '…';
        }
    }

    private static final class NormalizedBuilder {
        private final int limit;
        private final StringBuilder value;
        private int codePoints;
        private boolean pendingSpace;
        private int pendingNewlines;
        private boolean previousCarriageReturn;

        private NormalizedBuilder(int limit) {
            this.limit = limit;
            this.value = new StringBuilder(Math.min(limit * 2, 512));
        }

        private boolean append(int codePoint) {
            if (codePoint == '\r' || codePoint == '\n') {
                if (codePoint == '\n' && previousCarriageReturn) {
                    previousCarriageReturn = false;
                    return true;
                }
                previousCarriageReturn = codePoint == '\r';
                if (value.length() > 0) {
                    pendingNewlines = Math.min(2, pendingNewlines + 1);
                }
                pendingSpace = false;
                return true;
            }
            previousCarriageReturn = false;
            if (Character.isWhitespace(codePoint)) {
                pendingSpace = value.length() > 0 && pendingNewlines == 0;
                return true;
            }
            while (pendingNewlines > 0) {
                if (!appendValue('\n')) {
                    return false;
                }
                pendingNewlines--;
            }
            if (pendingSpace) {
                if (!appendValue(' ')) {
                    return false;
                }
                pendingSpace = false;
            }
            return appendValue(codePoint);
        }

        private boolean appendValue(int codePoint) {
            if (codePoints >= limit) {
                return false;
            }
            value.appendCodePoint(codePoint);
            codePoints++;
            return true;
        }

        private String text() {
            return value.toString();
        }
    }

    private static final class BoundedParts {
        private static final int DELIMITER_CODE_POINTS = 3;

        private final int limit;
        private final List<String> values = new ArrayList<>();
        private final Set<String> seen = new HashSet<>();
        private int codePoints;
        private boolean full;

        private BoundedParts(int limit) {
            this.limit = limit;
        }

        private void add(String value) {
            if (full || isBlank(value) || !seen.add(value)) {
                return;
            }
            if (values.size() >= MAX_PARTS) {
                full = true;
                return;
            }

            int delimiter = values.isEmpty() ? 0 : DELIMITER_CODE_POINTS;
            int remaining = limit - codePoints - delimiter;
            if (remaining <= 0) {
                full = true;
                return;
            }

            int originalLength = value.codePointCount(0, value.length());
            String clipped = clip(value, remaining);
            if (isBlank(clipped)) {
                full = true;
                return;
            }
            values.add(clipped);
            codePoints += delimiter + clipped.codePointCount(0, clipped.length());
            full = originalLength > remaining
                    || codePoints >= limit
                    || values.size() >= MAX_PARTS;
        }

        private boolean isEmpty() {
            return values.isEmpty();
        }

        private boolean isFull() {
            return full;
        }

        private String join() {
            return String.join(" · ", values);
        }
    }

    private static String clip(String value, int limit) {
        int count = value.codePointCount(0, value.length());
        if (count <= limit) {
            return value;
        }
        if (limit <= 1) {
            return "…";
        }
        int end = value.offsetByCodePoints(0, limit - 1);
        return value.substring(0, end) + '…';
    }

    private static String toolName(String type) {
        if (type == null) {
            return null;
        }
        if (type.endsWith("_call_output")) {
            return type.substring(0, type.length() - "_call_output".length());
        }
        if (type.endsWith("_call")) {
            return type.substring(0, type.length() - "_call".length());
        }
        return null;
    }
}
