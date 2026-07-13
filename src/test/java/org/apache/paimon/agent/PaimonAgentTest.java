package org.apache.paimon.agent;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class PaimonAgentTest {

    @Test
    void rejectsOnceForDashboardUrl() {
        assertThatThrownBy(
                        () ->
                                PaimonAgent.main(
                                        new String[] {"dashboard-url", "--once"}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("--once is only valid for the collector");
    }
}
