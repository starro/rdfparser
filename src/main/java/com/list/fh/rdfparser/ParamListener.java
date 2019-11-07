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

    @Value("${metaReadDiv:default_value}")
    private String metaReadDiv;

    @Value("${programId:default_value}")
    private String programId;

    @Value("${creationCycle:default_value}")
    private String creationCycle;

    @Value("${metaResourceDir:csv}")
    private String metaResourceDir;

    @Value("${dataResourceDir:default_value}")
    private String dataResourceDir;

    @Value("${toUploadDir:default_value}")
    private String toUploadDir;

    @Autowired
    private RdfParseService rdfParseService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        try {

            //메타데이터를 생성할 방식이 csv 파일과 DB로 구분된다.
            if (metaReadDiv.equals("csv")) {
                rdfParseService.createDcatFileByCSV(appHome, programId, creationCycle, metaResourceDir, toUploadDir);
            } else if(metaReadDiv.equals("DB")){
                rdfParseService.createDcatFileByDB(appHome, programId, creationCycle, toUploadDir);
            }

            //RDF파일로 변경할 csv파일 경로가 있으면 RDF파싱 시작
            if (!dataResourceDir.equals("default_value")) {
                rdfParseService.rdfParsing(appHome, programId, creationCycle, dataResourceDir, toUploadDir);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
