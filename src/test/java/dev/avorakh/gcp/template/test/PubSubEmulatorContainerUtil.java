package dev.avorakh.gcp.template.test;


import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CompletionException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.gax.rpc.TransportChannelProvider;

import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.spring.pubsub.core.PubSubConfiguration;
import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate;
import com.google.cloud.spring.pubsub.support.DefaultSubscriberFactory;

import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.TopicName;
import lombok.experimental.UtilityClass;

import lombok.extern.slf4j.Slf4j;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.testcontainers.gcloud.PubSubEmulatorContainer;
import org.testcontainers.utility.DockerImageName;


import io.grpc.ManagedChannelBuilder;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;

import com.google.cloud.spring.pubsub.core.publisher.PubSubPublisherTemplate;
import com.google.cloud.spring.pubsub.support.PublisherFactory;

import static com.google.protobuf.ByteString.copyFrom;

@Slf4j
@UtilityClass
public class PubSubEmulatorContainerUtil {

    public static final String DEFAULT_IMAGE_NAME = "gcr.io/google.com/cloudsdktool/google-cloud-cli:441.0.0-emulators";
    public static final String PROJECT_ID = "test-project";
    public static final String TOPIC_NAME = "test-topic";
    public static final String SUBSCRIPTION_NAME = "test-topic-sub";
    public static final String PUBSUB_EMULATOR_HOST = "PUBSUB_EMULATOR_HOST";

    private final ObjectMapper objectMapper = new ObjectMapper();

    public PubSubEmulatorContainer createContainer() {
        return createContainer(DEFAULT_IMAGE_NAME);
    }

    public PubSubEmulatorContainer createContainer(String imageName) {
        return new PubSubEmulatorContainer(DockerImageName.parse(imageName));
    }

    public void configureProperties(DynamicPropertyRegistry registry, PubSubEmulatorContainer container) {
        registry.add("spring.cloud.gcp.project-id", () -> "sample-project");
        registry.add("spring.cloud.gcp.credentials.enabled", () -> "false");
        registry.add("spring.cloud.gcp.pubsub.enabled", () -> "true");
        registry.add("spring.cloud.gcp.pubsub.emulator-host", container::getEmulatorEndpoint);
    }


    public void createTopic(PubSubEmulatorContainer emulator) throws IOException {
        var channelProvider = toTransportChannelProvider(emulator);

        var topicAdminSettings = TopicAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build();

        try (TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings)) {
            TopicName topicName = TopicName.of(PROJECT_ID, TOPIC_NAME);
            topicAdminClient.createTopic(topicName);
        }
    }


    public void createSubscription(PubSubEmulatorContainer emulator) throws IOException {
        var channelProvider = toTransportChannelProvider(emulator);

        var subscriptionAdminSettings = SubscriptionAdminSettings.newBuilder()
                .setTransportChannelProvider(channelProvider)
                .setCredentialsProvider(NoCredentialsProvider.create())
                .build();

        try (SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings)) {
            subscriptionAdminClient.createSubscription(
                    SubscriptionName.of(PROJECT_ID, SUBSCRIPTION_NAME),
                    TopicName.of(PROJECT_ID, TOPIC_NAME),
                    PushConfig.getDefaultInstance(),
                    10
            );
        }
    }

    public TransportChannelProvider toTransportChannelProvider(PubSubEmulatorContainer emulator) {
        String hostport = emulator.getEmulatorEndpoint();
        var channel = ManagedChannelBuilder.forTarget(hostport).usePlaintext().build();
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    }

    public PubSubPublisherTemplate createPubSubPublisherTemplate(TransportChannelProvider transportChannelProvider) {

        PublisherFactory publisherFactory = topicName -> {
            try {
                TopicName topic = TopicName.of(PROJECT_ID, topicName);
                return Publisher.newBuilder(topic)
                        .setChannelProvider(transportChannelProvider)
                        .setCredentialsProvider(NoCredentialsProvider.create())
                        .build();
            } catch (IOException e) {
                throw new RuntimeException("Failed to create publisher", e);
            }
        };

        return new PubSubPublisherTemplate(publisherFactory);
    }

    public PubSubSubscriberTemplate createPubSubSubscriberTemplate(TransportChannelProvider transportChannelProvider) {
        var configuration = new PubSubConfiguration();
        configuration.initialize(PROJECT_ID);

        var subscriberFactory = new DefaultSubscriberFactory(() -> PROJECT_ID, configuration);
        subscriberFactory.setChannelProvider(transportChannelProvider);
        subscriberFactory.setCredentialsProvider(NoCredentialsProvider.create());

        return new PubSubSubscriberTemplate(subscriberFactory);
    }

    public void sendEvent(PubSubPublisherTemplate pubSubPublisherTemplate, String topic, Map<String, Object> event, String eventId, String eventType) {
        PubsubMessage message = toPubsubMessage(event, eventId, eventType);

        pubSubPublisherTemplate.publish(topic, message)
                .handle((messageId, throwable) -> {
                    if (throwable != null) {
                        if (throwable instanceof CompletionException exception && throwable.getCause() != null) {
                            throwable = exception;
                        }
                        log.error("Unable to publish Pub/Sub message due to error. message:[{}], error:[{}].", message, throwable, throwable);

                    } else {
                        log.info("Pub/Sub message published successfully.message:[{}], messageId:[{}]", message, messageId);
                    }
                    return null;
                })
                .join();

    }

    public PubsubMessage toPubsubMessage(Map<String, Object> event, String eventId, String eventType) {
        byte[] jsonMessageBytes = toJsonBytes(event);

        return PubsubMessage.newBuilder()
                .setData(copyFrom(jsonMessageBytes))
                .putAttributes("eventId", eventId)
                .putAttributes("eventType", eventType)
                .build();
    }

    private byte[] toJsonBytes(Map<String, Object> event) {
        try {
            return objectMapper.writeValueAsBytes(event);
        } catch (JsonProcessingException e) {
            log.error("Unable write event.", e);
            throw new RuntimeException(e);
        }
    }
}