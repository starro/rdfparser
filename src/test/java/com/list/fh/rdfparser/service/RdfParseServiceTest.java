package com.list.fh.rdfparser.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
class RdfParseServiceTest {

    @Autowired
    private RdfParseService rdfParseService;

    @Test
    public void test1() throws Exception {
//        rdfParseService.rdfParsing();
//        rdfParseService.rdfParsing("D:\\test.csv", "");
//        rdfParseService.readCSV("D:\\test.csv");
    }

    @Test
    public void test2() throws Exception {
//        rdfParseService.createDcatFileByCSV("D:\\dcat_master.csv","D:\\tmp\\sample_dcat_master.dcat");
        rdfParseService.rdfParsing("D:\\italy-tot-1996-2008-pubexp.csv", "D:\\tmp\\newLargeFile.rdf", "http://slb-71156.gov-ncloudslb.com/");
    }

    @Test
    public void test3() throws Exception {
//        System.out.println("쿼리 시작");
//        rdfParseService.createDcat("tableKey");

    }
}