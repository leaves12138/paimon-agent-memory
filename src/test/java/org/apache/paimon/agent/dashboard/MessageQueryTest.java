package org.apache.paimon.agent.dashboard;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class MessageQueryTest {

    @Test
    void recognizesConversationTextGeneratedImagesAndAttachmentsButNotInternalRoles() {
        assertThat(MessageQuery.isConversationalMessage("user", "message")).isTrue();
        assertThat(MessageQuery.isConversationalMessage("assistant", "assistant")).isTrue();
        assertThat(
                        MessageQuery.isConversationalMessage(
                                "assistant", "image_generation_end"))
                .isTrue();
        assertThat(MessageQuery.isConversationalMessage("attachment", "attachment"))
                .isTrue();
        assertThat(MessageQuery.isConversationalMessage("attachment", null)).isTrue();
        assertThat(MessageQuery.isConversationalMessage("user", "attachment")).isTrue();
        assertThat(MessageQuery.isConversationalMessage("assistant", "attachment"))
                .isTrue();
        assertThat(MessageQuery.isConversationalMessage(null, "attachment")).isTrue();

        assertThat(MessageQuery.isConversationalMessage("system", "attachment")).isFalse();
        assertThat(MessageQuery.isConversationalMessage("developer", "message")).isFalse();
        assertThat(MessageQuery.isConversationalMessage("tool", "attachment")).isFalse();
        assertThat(MessageQuery.isConversationalMessage("assistant", "custom_tool_call"))
                .isFalse();
        assertThat(MessageQuery.isConversationalMessage("attachment", "tool_result"))
                .isFalse();
    }
}
