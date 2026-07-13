package org.apache.paimon.agent.service;

import org.apache.paimon.agent.model.SessionBatch;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Immutable view of rows collected locally but not yet confirmed as uploaded to Paimon. */
public final class PendingDataSnapshot {

    private static final PendingDataSnapshot EMPTY =
            new PendingDataSnapshot(-1L, Collections.emptyList());

    private final long commitIdentifier;
    private final List<SessionBatch> batches;

    public PendingDataSnapshot(long commitIdentifier, List<SessionBatch> batches) {
        this.commitIdentifier = commitIdentifier;
        this.batches =
                Collections.unmodifiableList(
                        new ArrayList<>(batches == null ? Collections.emptyList() : batches));
    }

    public static PendingDataSnapshot empty() {
        return EMPTY;
    }

    public long commitIdentifier() {
        return commitIdentifier;
    }

    public List<SessionBatch> batches() {
        return batches;
    }

    public boolean isEmpty() {
        return batches.isEmpty();
    }
}
