package dev.avorakh.gcp.template.controller;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultHandlers.print;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.avorakh.gcp.template.resource.AppInfoMeta;
import dev.avorakh.gcp.template.svc.AppInfoService;
import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;

import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

@WebMvcTest(AppInfoController.class)
class AppInfoControllerTest {
    @Autowired
    private MockMvc mockMvc;
    @MockitoBean
    private AppInfoService appInfoService;

    @Test
    public void shouldSuccessfullyReturnOkSCAndSuccessWithTrueInBody() throws Exception {

        var expectedName = "test";
        boolean expectedSuccess = true;

        when(appInfoService.getAppInfo()).thenReturn(new AppInfoMeta(expectedSuccess, expectedName));

        var request = get("/app/info").accept(MediaType.APPLICATION_JSON);

        mockMvc.perform(request)
                .andDo(print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.success").isBoolean())
                .andExpect(jsonPath("$.success").value(expectedSuccess))
                .andExpect(jsonPath("$.name").isString())
                .andExpect(jsonPath("$.name").value(expectedName));
    }
}