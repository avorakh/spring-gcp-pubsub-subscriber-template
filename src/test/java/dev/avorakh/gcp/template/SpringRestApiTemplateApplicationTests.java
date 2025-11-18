package dev.avorakh.gcp.template;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.spring.core.GcpProjectIdProvider;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
class SpringRestApiTemplateApplicationTests {

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
