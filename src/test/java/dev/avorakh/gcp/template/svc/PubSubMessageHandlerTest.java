package dev.avorakh.gcp.template.svc;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.spring.pubsub.core.publisher.PubSubPublisherTemplate;
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import com.google.cloud.spring.pubsub.support.AcknowledgeablePubsubMessage;
import dev.avorakh.gcp.template.test.PubSubEmulatorContainerUtil;
import org.junit.jupiter.api.*;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.testcontainers.gcloud.PubSubEmulatorContainer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import static org.awaitility.Awaitility.await;

import static dev.avorakh.gcp.template.test.PubSubEmulatorContainerUtil.*;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

class PubSubMessageHandlerTest {
    private static final PubSubEmulatorContainer PUBSUB_EMULATOR = PubSubEmulatorContainerUtil.createContainer();

    private PubSubPublisherTemplate pubSubPublisherTemplate;
    private PubSubSubscriberTemplate subscriberTemplate;
    private PubSubInboundChannelAdapter adapter;

    private PubSubMessageHandler sut;

    @BeforeAll
    static void beforeAll() throws IOException {
        PUBSUB_EMULATOR.start();
        String emulatorEndpoint = PUBSUB_EMULATOR.getEmulatorEndpoint();
        System.setProperty(PUBSUB_EMULATOR_HOST, emulatorEndpoint);
        PubSubEmulatorContainerUtil.createTopic(PUBSUB_EMULATOR);
        PubSubEmulatorContainerUtil.createSubscription(PUBSUB_EMULATOR);
    }

    @AfterAll
    static void afterAll() {
        PUBSUB_EMULATOR.stop();
        System.clearProperty(PUBSUB_EMULATOR_HOST);
    }

    @BeforeEach
    void setUp() {
        TransportChannelProvider transportChannelProvider = PubSubEmulatorContainerUtil.toTransportChannelProvider(PUBSUB_EMULATOR);
        pubSubPublisherTemplate = PubSubEmulatorContainerUtil.createPubSubPublisherTemplate(transportChannelProvider);
        subscriberTemplate = PubSubEmulatorContainerUtil.createPubSubSubscriberTemplate(transportChannelProvider);
        sut = spy(new PubSubMessageHandler());
        DirectChannel inputChannel = new DirectChannel();
        inputChannel.subscribe(sut);
        adapter = new PubSubInboundChannelAdapter(subscriberTemplate, SUBSCRIPTION_NAME);
        adapter.setOutputChannel(inputChannel);
        adapter.setAutoStartup(false);

    }

    @AfterEach
    void tearDown() {
        await().until(() -> subscriberTemplate.pullAndAck(SUBSCRIPTION_NAME, 1000, true), hasSize(0));
    }

    @Test
    public void shouldSuccessfullyHandlePubSubMessage() {
        String eventId = UUID.randomUUID().toString();
        String eventType = "TestEvent";
        Map<String, Object> event = Map.of(
                "eventId", eventId,
                "eventType", eventType,
                "payload", Map.of("created", System.currentTimeMillis())
        );
        PubSubEmulatorContainerUtil.sendEvent(pubSubPublisherTemplate, TOPIC_NAME, event, eventId, eventType);

        adapter.start();

        verify(sut, timeout(2000).atLeastOnce()).handleMessage(any(Message.class));
    }
}