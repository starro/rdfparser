package com.list.fh.rdfparser.service;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

@Slf4j
@Service
public class RdfParseService {

    public static final Logger logger = LoggerFactory.getLogger(RdfParseService.class);

    public void createDcatFileByCSV(String source, String target) throws Exception {

        File csvFile = new File(source);

        CsvMapper mapper = new CsvMapper();
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();

        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(csvSchema)
                .readValues(csvFile);

        //프리픽스 선언
//        String defaultPrefix = "@prefix : <http://example.org/> .";
        String rdfPrefix = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .";
        String rdfsPrefix = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .";
        String dcatPrefix = "@prefix dcat: <http://www.w3.org/ns/dcat#> .";
        String dctPreFix = "@prefix dct: <http://purl.org/dc/terms/> .";
        String foafPreFix = "@prefix foaf: <http://xmlns.com/foaf/0.1/> .";

        //DATASET 제작 시작
        //dcat내용 제작을 위한 변수들
        String catalogDataSet = "DATASET";
        String catalogDistribution = "Distribution";
        String catalogDataService = "DataService";

        String seperator = ":";
        String tab = "\t";
        String space = " ";
        String semiCol = ";";
        String newLine = "\n";
        String terminator = ".";

        LocalDateTime now;

        //dcat 생성 시작
        while (it.hasNext()) {
            Map<String, String> rowAsMap = it.next();

            //파일명 공통 생성부
            now = LocalDateTime.now();
            LocalDateTime nowTIme = LocalDateTime.of(now.getYear(),
                    now.getMonth(), now.getDayOfMonth(), now.getHour(), now.getMinute(), now.getSecond());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            nowTIme.format(formatter);
            String fileCreateTIme = nowTIme.toString().replaceAll("[-T:]", "");

            //키워드는 쉼표로 분리한다.
            String Keywords = rowAsMap.get("DATASET_keyword");
            String[] keywords = Keywords.split(",");
            String dcatKeywords = "";
            for (String modString : keywords) {
                dcatKeywords = dcatKeywords + "\"" + modString.trim() + "\"" + "@" + rowAsMap.get("DATASET_language") + space;
            }

            //키워드를 분리시켜서 더블쿼테이션과 랭귀지를 단다.
            dcatKeywords = dcatKeywords.replaceAll(" ", ", ");
            dcatKeywords = dcatKeywords.trim();
            dcatKeywords = dcatKeywords.substring(0, dcatKeywords.length() - 1);

            //DCAT 파일 내용 제작
            String dcatContent =
                    rdfPrefix + newLine
                            + rdfsPrefix + newLine
                            + dcatPrefix + newLine
                            + dctPreFix + newLine
                            + foafPreFix + newLine
                            + newLine
                            + seperator + catalogDataSet + newLine
                            + tab + "a dcat" + seperator + catalogDataSet + space + semiCol + newLine
                            + tab + "dct" + seperator + "title" + space + "\"" + rowAsMap.get("DATASET_title") + "\"" + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine
                            + tab + "dct" + seperator + "description" + space + "\"" + rowAsMap.get("DATASET_description") + "\"" + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "keyword" + space + dcatKeywords + space + semiCol + newLine
                            + tab + "dct" + seperator + "creator" + space + rowAsMap.get("DATASET_creator") + space + semiCol + newLine
                            + tab + "dct" + seperator + "issued" + space + "\"" + rowAsMap.get("DATASET_issued") + "\"^^xsd:date" + space + semiCol + newLine
                            + tab + "dct" + seperator + "modified" + space + "\"" + rowAsMap.get("DATASET_modified") + "\"^^xsd:date" + space + semiCol + newLine
                            + tab + "dcat" + seperator + "contactPoint" + space + rowAsMap.get("DATASET_contactPoint") + space + semiCol + newLine
                            + tab + "dct" + seperator + "language" + space + rowAsMap.get("DATASET_language") + space + semiCol + newLine
                            + tab + "dct" + seperator + "conformsTo" + space + rowAsMap.get("DATASET_conformsTo") + space + semiCol + newLine
                            + tab + "dct" + seperator + "accessRights" + space + rowAsMap.get("DATASET_accessRights") + space + semiCol + newLine
                            + tab + "dct" + seperator + "publisher" + space + rowAsMap.get("DATASET_publisher") + space + semiCol + newLine
                            + tab + "dct" + seperator + "identifier" + space + rowAsMap.get("DATASET_identifier") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "theme" + space + rowAsMap.get("DATASET_theme") + space + semiCol + newLine
                            + tab + "dct" + seperator + "type" + space + rowAsMap.get("DATASET_type") + space + semiCol + newLine
                            + tab + "dct" + seperator + "relation" + space + rowAsMap.get("DATASET_relation") + space + semiCol + newLine
                            + tab + "dct" + seperator + "license" + space + rowAsMap.get("DATASET_license") + space + semiCol + newLine
                            + tab + "dct" + seperator + "spatial" + space + rowAsMap.get("DATASET_spatial") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "distribution" + space + rowAsMap.get("DATASET_distribution") + space + semiCol + newLine
                            + tab + terminator + newLine
                            + newLine
                            + seperator + catalogDistribution + newLine
                            + tab + "a dcat" + seperator + catalogDistribution + space + semiCol + newLine
                            + tab + "dct" + seperator + "title" + space + "\"" + rowAsMap.get("Distribution_title") + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine
                            + tab + "dct" + seperator + "description" + space + "\"" + rowAsMap.get("Distribution_description") + "\"" + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine
                            + tab + "dct" + seperator + "issued" + space + "\"" + rowAsMap.get("Distribution_issued") + "\"^^xsd:date" + space + semiCol + newLine
                            + tab + "dct" + seperator + "license" + space + rowAsMap.get("Distribution_license") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "accessUrl" + space + rowAsMap.get("Distribution_accessURL") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "accessService" + space + rowAsMap.get("Distribution_accessService") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "downloadUrl" + space + rowAsMap.get("Distribution_downloadURL") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "spatialResolutionlnMeters" + space + rowAsMap.get("Distribution_spatialResolutionlnMeters") + space + semiCol + newLine
                            + tab + "dct" + seperator + "format" + space + rowAsMap.get("Distribution_format") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "compressFormat" + space + rowAsMap.get("Distribution_compressFormat") + space + semiCol + newLine
                            + tab + terminator + newLine
                            + newLine
                            + seperator + catalogDataService + newLine
                            + tab + "a dcat" + seperator + catalogDataService + space + semiCol + newLine
                            + tab + "dcat" + seperator + "endpointUrl" + space + rowAsMap.get("DataService_endpointURL") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "endpointDescripton" + space + space + rowAsMap.get("DataService_endpointDescripton") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "servesDataset" + space + space + rowAsMap.get("DataService_servesDataset") + space + semiCol + newLine
                            + tab + terminator;

            String file_path = target;
            int lastIndex = file_path.lastIndexOf(File.separator);
            String fileName = file_path.substring(lastIndex + 1);
            String filePath = file_path.substring(0, lastIndex + 1);

            File targetPath = new File(filePath);
            if (!targetPath.exists()) {
                targetPath.mkdirs();
            }

            //파일이 생성된다
            FileOutputStream dcatFos = new FileOutputStream(target);
            byte[] dcatByte = dcatContent.getBytes();
            dcatFos.write(dcatByte);
            dcatFos.flush();
            dcatFos.close();
        }
    }

    public void rdfParsing(String source, String target, String url) throws Exception {
        File csvFile = new File(source);

        CsvMapper mapper = new CsvMapper();
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();

        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(csvSchema)
                .readValues(csvFile);

        LocalDate currentDate = LocalDate.now();

        String file_path = target;
        int lastIndex = file_path.lastIndexOf(File.separator);
        String fileName = file_path.substring(lastIndex + 1);
        String filePath = file_path.substring(0, lastIndex + 1);

        String sourcePath = source;

        int sourceIndex = sourcePath.lastIndexOf(File.separator);
        String sourceFileName = sourcePath.substring(sourceIndex + 1);

        String title = sourceFileName.replaceAll(".csv", "");

        Map<String, String> headerInfo = it.next();

        ModelBuilder builder = new ModelBuilder();
        builder
                .setNamespace("dataset", url);

        //rdf 빌더에 요소를 더하는 부분
        int i = 0;
        while (it.hasNext()) {
            i++;
            Map<String, String> rowAsMap = it.next();
            builder.subject("dataset:" + "dataset?q=" + title + "&" + String.valueOf(i));

            for (String element : rowAsMap.keySet()) {
                builder.add("dataset:" + element, rowAsMap.get(element).toString());
            }
        }

        Model model = builder.build();

        File targetPath = new File(filePath);
        if (!targetPath.exists()) {
            targetPath.mkdirs();
        }

        FileOutputStream fos = new FileOutputStream(target);

        Rio.write(model, fos, RDFFormat.RDFXML);
        fos.close();
    }
}