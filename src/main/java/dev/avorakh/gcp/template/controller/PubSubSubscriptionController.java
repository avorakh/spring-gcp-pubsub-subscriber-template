package dev.avorakh.gcp.template.controller;

import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/pubsub")
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class PubSubSubscriptionController {

    PubSubInboundChannelAdapter messageAdapter;

    @PostMapping("/start")
    public ResponseEntity<Void> start() {
        messageAdapter.start();
        return ResponseEntity.accepted().build();
    }

    @PostMapping("/stop")
    public ResponseEntity<Void> stop() {
        messageAdapter.stop();
        return ResponseEntity.accepted().build();
    }
}
