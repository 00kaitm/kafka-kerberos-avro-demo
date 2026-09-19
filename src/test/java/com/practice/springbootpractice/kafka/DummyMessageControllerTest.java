package com.practice.springbootpractice.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(DummyMessageController.class)
class DummyMessageControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private DummyProducer producer;

    @Test
    void postingAMessageHandsTheBodyToTheProducerAndRespondsQueued() throws Exception {
        mockMvc.perform(post("/dummy-topic/messages")
                        .contentType(MediaType.TEXT_PLAIN)
                        .content("hello after reboot"))
                .andExpect(status().isOk())
                .andExpect(content().string("queued"));

        verify(producer).send("hello after reboot");
    }

    @Test
    void postingWithoutABodyIsRejected() throws Exception {
        mockMvc.perform(post("/dummy-topic/messages")
                        .contentType(MediaType.TEXT_PLAIN))
                .andExpect(status().isBadRequest());
    }

    @Test
    void getIsNotAllowedOnTheMessagesEndpoint() throws Exception {
        mockMvc.perform(get("/dummy-topic/messages"))
                .andExpect(status().isMethodNotAllowed());
    }
}
