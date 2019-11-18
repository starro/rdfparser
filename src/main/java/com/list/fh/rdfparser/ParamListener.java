package com.list.fh.rdfparser;

import com.list.fh.rdfparser.service.RdfParseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

@Component
public class ParamListener implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${type:default_value}")
    private String type;

    @Value("${source:default_value}")
    private String source;

    @Value("${target:default_value}")
    private String target;

    @Value("${url:default_value}")
    private String url;

    @Autowired
    private RdfParseService rdfParseService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {

        try {
            //메타데이터를 생성할 방식이 csv 파일과 DB로 구분된다.
            if (type.equals("dcat")) {
                rdfParseService.createDcatFileByCSV(source, target);
            } else if (type.equals("rdf")) {
                rdfParseService.rdfParsing(source, target, url);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
