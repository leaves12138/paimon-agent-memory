package org.apache.paimon.agent.source;

import org.apache.paimon.agent.model.AttachmentPayload;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** JSON enriched with an attachment manifest plus its index-aligned payloads. */
public final class ResolvedAttachments {

    private final String contentJson;
    private final List<AttachmentPayload> payloads;

    public ResolvedAttachments(String contentJson, List<AttachmentPayload> payloads) {
        this.contentJson = contentJson;
        this.payloads = Collections.unmodifiableList(new ArrayList<>(payloads));
    }

    public String contentJson() {
        return contentJson;
    }

    public List<AttachmentPayload> payloads() {
        return payloads;
    }
}
