package com.practice.springbootpractice.kafka;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class DummyMessageController {

    private final DummyProducer producer;

    public DummyMessageController(DummyProducer producer) {
        this.producer = producer;
    }

    @PostMapping("/dummy-topic/messages")
    public String send(@RequestBody String message) {
        producer.send(message);
        return "queued";
    }
}
