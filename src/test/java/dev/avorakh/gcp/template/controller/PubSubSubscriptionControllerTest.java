package dev.avorakh.gcp.template.controller;

import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(PubSubSubscriptionController.class)
class PubSubSubscriptionControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private PubSubInboundChannelAdapter messageAdapter;

    @Test
    void shouldSuccessfullyStart() throws Exception {
        doNothing().when(messageAdapter).start();

        mockMvc.perform(post("/api/pubsub/start"))
                .andExpect(status().isAccepted());

        verify(messageAdapter).start();
    }

    @Test
    void shouldSuccessfullyStop() throws Exception {
        doNothing().when(messageAdapter).stop();

        mockMvc.perform(post("/api/pubsub/stop"))
                .andExpect(status().isAccepted());

        verify(messageAdapter).stop();
    }
}