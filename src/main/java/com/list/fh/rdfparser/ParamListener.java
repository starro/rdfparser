package com.list.fh.rdfparser;

import com.list.fh.rdfparser.service.RdfParseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Component
public class ParamListener implements ApplicationListener<ContextRefreshedEvent> {

    @Value("${param1:D:\\test.csv}")
    private String param1;

    @Value("${param2:defaultValue}")
    private String param2;

    @Autowired
    private RdfParseService rdfParseService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
//        try {
//            rdfParseService.rdfParsing(param1, param2);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}
