package com.list.fh.rdfparser.service;

import com.list.fh.rdfparser.mapper.DcatInfoMapper;
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
    private DcatInfoMapper mapper;

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
//        rdfParseService.processExecute("D:\\test.csv", "");
    }

    @Test
    public void test3() throws Exception {
        System.out.println("쿼리 시작");
//        rdfParseService.createDcat("tableKey");

    }
}