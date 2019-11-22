package com.list.fh.rdfparser.service;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.junit.jupiter.api.Assertions.*;

@Slf4j
@SpringBootTest
@RunWith(SpringRunner.class)
class RdfParseForLargeDataServiceTest {

    @Autowired
    private RdfParseForLargeDataService rdfParseForLargeDataService;

    @Test
    public void test1() throws Exception {
        rdfParseForLargeDataService.rdfParsing("D:\\italy-tot-1996-2008-pubexp.csv", "D:\\tmp\\newLargeFile.rdf", "http://slb-71156.gov-ncloudslb.com/");
    }

}