package org.apache.paimon.agent.source;

/** One UTF-8 JSONL record with stable byte offsets in its source file. */
public final class JsonlRecord {

    private final long startOffset;
    private final long endOffset;
    private final String json;
    private final boolean lineTerminated;

    public JsonlRecord(long startOffset, long endOffset, String json, boolean lineTerminated) {
        this.startOffset = startOffset;
        this.endOffset = endOffset;
        this.json = json;
        this.lineTerminated = lineTerminated;
    }

    public long startOffset() {
        return startOffset;
    }

    public long endOffset() {
        return endOffset;
    }

    public String json() {
        return json;
    }

    public boolean lineTerminated() {
        return lineTerminated;
    }
}
