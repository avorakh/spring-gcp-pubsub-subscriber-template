package dev.avorakh.gcp.template.svc;

import dev.avorakh.gcp.template.resource.AppInfoMeta;
import org.springframework.stereotype.Service;

@Service
public class AppInfoServiceImpl implements AppInfoService{
    @Override
    public AppInfoMeta getAppInfo() {
        return new AppInfoMeta(true,"template");
    }
}
