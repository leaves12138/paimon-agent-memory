package org.apache.paimon.agent.sink;

import org.apache.paimon.agent.model.ChatMessage;

/** Checked consumer used while streaming potentially large restored messages and BLOBs. */
@FunctionalInterface
public interface ChatMessageConsumer {

    void accept(ChatMessage message) throws Exception;
}
