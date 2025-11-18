package dev.avorakh.gcp.template.controller;

import dev.avorakh.gcp.template.resource.AppInfoMeta;
import dev.avorakh.gcp.template.svc.AppInfoService;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
@RequiredArgsConstructor
@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AppInfoController {

    AppInfoService appInfoService;

    @GetMapping("/app/info")
    public AppInfoMeta getAppInfo(){
        return appInfoService.getAppInfo();
    }
}
