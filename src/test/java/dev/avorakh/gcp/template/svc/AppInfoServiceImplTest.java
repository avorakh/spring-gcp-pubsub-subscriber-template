package dev.avorakh.gcp.template.svc;

import dev.avorakh.gcp.template.resource.AppInfoMeta;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class AppInfoServiceImplTest {

   private AppInfoService sut;

    @BeforeEach
    void init() {
        sut = new AppInfoServiceImpl();
    }

    @Test
    void shouldSuccessfully_getAppInfo() {

        var expected =  new AppInfoMeta(true, "template");

        var actual = sut.getAppInfo();

        Assertions.assertEquals(
                expected,
                actual,
                "Should be equaled the expected value");
    }
}