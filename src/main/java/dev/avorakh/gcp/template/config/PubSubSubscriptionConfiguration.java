package dev.avorakh.gcp.template.config;

import com.google.cloud.spring.pubsub.core.subscriber.PubSubSubscriberTemplate;
import com.google.cloud.spring.pubsub.integration.inbound.PubSubInboundChannelAdapter;
import dev.avorakh.gcp.template.svc.PubSubMessageHandler;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@Slf4j
@Configuration
public class PubSubSubscriptionConfiguration {

    @Bean
    public MessageChannel pubsubInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public PubSubInboundChannelAdapter messageAdapter(
            @Qualifier("pubsubInputChannel") MessageChannel inputChannel,
            PubSubSubscriberTemplate subscriberTemplate,
            @Value("${app.pubsub.subscription}") String subscription) {
        PubSubInboundChannelAdapter adapter =
                new PubSubInboundChannelAdapter(subscriberTemplate, subscription);
        adapter.setOutputChannel(inputChannel);
        adapter.setAutoStartup(true);
        return adapter;
    }


    @Bean
    @ServiceActivator(inputChannel = "pubsubInputChannel")
    public MessageHandler messageReceiver() {
        return new PubSubMessageHandler();
    }
}
