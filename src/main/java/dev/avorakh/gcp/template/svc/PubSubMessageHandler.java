package dev.avorakh.gcp.template.svc;

import com.google.cloud.spring.pubsub.support.BasicAcknowledgeablePubsubMessage;
import com.google.cloud.spring.pubsub.support.GcpPubSubHeaders;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

@Slf4j
public class PubSubMessageHandler implements MessageHandler {
    @Override
    public void handleMessage(Message<?> message) throws MessagingException {
        Object payload = message.getPayload();
        var payloadData = new String((byte[]) payload);
        log.info("Message arrived. Payload: [{}], headers: [{}]", payloadData, message.getHeaders());

        BasicAcknowledgeablePubsubMessage originalMessage =
                message.getHeaders()
                        .get(GcpPubSubHeaders.ORIGINAL_MESSAGE, BasicAcknowledgeablePubsubMessage.class);
        if (originalMessage != null) {
            originalMessage.ack();
        } else {
            log.warn("No ORIGINAL_MESSAGE header found; cannot ack.");
        }
    }
}
