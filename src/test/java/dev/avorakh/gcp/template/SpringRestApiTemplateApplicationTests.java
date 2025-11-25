package dev.avorakh.gcp.template;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.spring.core.GcpProjectIdProvider;
import dev.avorakh.gcp.template.test.PubSubEmulatorContainerUtil;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.gcloud.PubSubEmulatorContainer;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class SpringRestApiTemplateApplicationTests {
    private static final PubSubEmulatorContainer PUBSUB_EMULATOR = PubSubEmulatorContainerUtil.createContainer();
    @BeforeAll
    static void beforeAll() {
        PUBSUB_EMULATOR.start();
    }

    @AfterAll
    static void afterAll() {
        PUBSUB_EMULATOR.stop();
    }

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        PubSubEmulatorContainerUtil.configureProperties(registry, PUBSUB_EMULATOR);
    }
    @Autowired
    private CredentialsProvider googleCredentials;

    @Autowired
    private GcpProjectIdProvider gcpProjectIdProvider;

    @Test
    void contextLoads() {
        // context load check
        assertNotNull(googleCredentials, "googleCredentials bean should be present");
        assertNotNull(gcpProjectIdProvider, "gcpProjectIdProvider bean should be present");
    }

}
