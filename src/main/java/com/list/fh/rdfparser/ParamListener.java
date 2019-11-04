package com.list.fh.rdfparser;

import com.list.fh.rdfparser.service.RdfParseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
public class ParamListener implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${appHome:/appdata}")
    private String appHome;

    @Value("${programId:test}")
    private String programId;

    @Value("${creationCycle:HH}")
    private String creationCycle;

    @Value("${resourceDir:tmp}")
    private String resourceDir;

    @Value("${toUpload:to_upload_data}")
    private String toUpload;

    @Autowired
    private RdfParseService rdfParseService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        try {
            rdfParseService.createFile(appHome, programId, creationCycle, resourceDir, toUpload);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
