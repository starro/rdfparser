package com.list.fh.rdfparser.service;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.list.fh.rdfparser.mapper.DcatInfoMapper;
import com.list.fh.rdfparser.model.DcatDataserviceInfo;
import com.list.fh.rdfparser.model.DcatDatasetInfo;
import com.list.fh.rdfparser.model.DcatDistributionInfo;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class RdfParseService {

    public static final Logger logger = LoggerFactory.getLogger(RdfParseService.class);

    @Autowired
    private DcatInfoMapper mapper;

    public void createDcatFileByDB(String appHome, String programId, String creationCycle, String toUploadDir) throws Exception {

        List<DcatDatasetInfo> dcatInfoList = mapper.selectDatasetInfoList();
        List<DcatDistributionInfo> dcatDistributionInfoList = mapper.selectDistributionInfoList();
        List<DcatDataserviceInfo> dcatDataserviceInfoList = mapper.selectDataserviceInfoList();

        //프리픽스 선언
        String defaultPrefix = "@prefix : <http://example.org/> .";
        String rdfPrefix = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .";
        String rdfsPrefix = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .";
        String dcatPrefix = "@prefix dcat: <http://www.w3.org/ns/dcat#> .";
        String dctPreFix = "@prefix dct: <http://purl.org/dc/terms/> .";

        //파일경로명 제작
        String targetPath = toUploadDir + File.separator + programId + File.separator + creationCycle;

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

        //데이터셋 생성
        //임시로 DcatSeq로 생성 파일들 구분
        String datasetTargetPath = targetPath + File.separator + catalogDataSet;

        for (DcatDatasetInfo dcatInfo : dcatInfoList) {
            now = LocalDateTime.now();
            LocalDateTime nowTIme = LocalDateTime.of(now.getYear(),
                    now.getMonth(), now.getDayOfMonth(), now.getHour(), now.getMinute(), now.getSecond());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            nowTIme.format(formatter);

            String fileCreateTIme = nowTIme.toString().replaceAll("[-T:]", "");

            String fileName = programId + "_" + fileCreateTIme;

            String filePath = datasetTargetPath + File.separator + programId;

            //파일경로 생성
            File datasetFile = new File(filePath);

            if (!datasetFile.exists()) {
                logger.info("파일경로생성");
                datasetFile.mkdirs();
            }
            //DCAT 파일 내용 제작
            String datasetContent =
                    defaultPrefix + newLine
                            + rdfPrefix + newLine
                            + rdfsPrefix + newLine
                            + dcatPrefix + newLine
                            + dctPreFix + newLine
                            + newLine
                            + seperator + catalogDataSet + newLine
                            + tab + "a dcat" + seperator + catalogDataSet + space + semiCol + newLine
                            + tab + "dct" + seperator + "accessRights" + space + dcatInfo.getDatasetAccessRights() + space + semiCol + newLine
                            + tab + "dct" + seperator + "conformsTo" + space + dcatInfo.getDatasetConformsTo() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "contactPoint" + space + dcatInfo.getDatasetContactPoint() + space + semiCol + newLine
                            + tab + "dct" + seperator + "creator" + space + dcatInfo.getDatasetCreator() + space + semiCol + newLine
                            + tab + "dct" + seperator + "description" + space + dcatInfo.getDatasetDescription() + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "title" + space + dcatInfo.getDatasetTitle() + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "issued" + space + dcatInfo.getDatasetIssued() + space + semiCol + newLine
                            + tab + "dct" + seperator + "modified" + space + dcatInfo.getDatasetModified() + space + semiCol + newLine
                            + tab + "dct" + seperator + "language" + space + dcatInfo.getDatasetLanguage() + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "publisher" + space + dcatInfo.getDatasetPublisher() + space + semiCol + newLine
                            + tab + "dct" + seperator + "identifier" + space + dcatInfo.getDatasetIdentifier() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "theme" + space + dcatInfo.getDatasetTheme() + space + semiCol + newLine
                            + tab + "dct" + seperator + "type" + space + dcatInfo.getDatasetType() + space + semiCol + newLine
                            + tab + "dct" + seperator + "relation" + space + dcatInfo.getDatasetRelation() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "keyword" + space + dcatInfo.getDatasetKeyword() + space + semiCol + newLine
                            + tab + "dct" + seperator + "license" + space + dcatInfo.getDatasetLicense() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "distribution" + space + dcatInfo.getDatasetDistribution() + space + semiCol + newLine
                            + tab + "dct" + seperator + "spatial" + space + dcatInfo.getDatasetSpatial() + space + semiCol + newLine
                            + tab + terminator;

            logger.info(datasetContent);

            //파일 경로만 생성되었다가 파일이 생성된다
            FileOutputStream fos = new FileOutputStream(datasetFile + File.separator + fileName + ".dcat");
            byte[] content = datasetContent.getBytes();
            fos.write(content);
            fos.flush();
            fos.close();
        }

        //디스트리뷰션 생성
        String distributionTargetPath = targetPath + File.separator + catalogDistribution;

        for (DcatDistributionInfo dcatInfo : dcatDistributionInfoList) {
            now = LocalDateTime.now();
            LocalDateTime nowTIme = LocalDateTime.of(now.getYear(),
                    now.getMonth(), now.getDayOfMonth(), now.getHour(), now.getMinute(), now.getSecond());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            nowTIme.format(formatter);

            String fileCreateTIme = nowTIme.toString().replaceAll("[-T:]", "");

            String fileName = programId + "_" + fileCreateTIme;

            //임시로 DcatSeq로 생성 파일들 구분
            String filePath = distributionTargetPath + File.separator + programId;

            // 파일경로 생성
            File distributionFile = new File(filePath);

            if (!distributionFile.exists()) {
                logger.info("파일경로생성");
                distributionFile.mkdirs();
            }

            //DCAT 파일 내용 제작
            String distributionContent =
                    defaultPrefix + newLine
                            + rdfPrefix + newLine
                            + rdfsPrefix + newLine
                            + dcatPrefix + newLine
                            + dctPreFix + newLine
                            + newLine
                            + seperator + catalogDistribution + newLine
                            + tab + "a dcat" + seperator + catalogDistribution + space + semiCol + newLine
                            + tab + "dct" + seperator + "title" + space + dcatInfo.getDistributionTitle() + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "description" + space + dcatInfo.getDistributionDescription() + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "issued" + space + dcatInfo.getDistributionIssued() + space + semiCol + newLine
                            + tab + "dct" + seperator + "license" + space + dcatInfo.getDistributionLicense() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "accessUrl" + space + dcatInfo.getDistributionAccessUrl() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "accessService" + space + dcatInfo.getDistributionAccessService() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "downloadUrl" + space + dcatInfo.getDistributionDownloadUrl() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "spatialResolutionlnMeters" + space + dcatInfo.getDistributionSpatialResolutionlnMeters() + space + semiCol + newLine
                            + tab + "dct" + seperator + "format" + space + dcatInfo.getDistributionFormat() + space + semiCol + newLine
                            + tab + "dcat" + seperator + "compressFormat" + space + dcatInfo.getDistributionCompressFormat() + space + semiCol + newLine
                            + tab + terminator;

            logger.info(distributionContent);

            //파일 경로만 생성되었다가 파일이 생성된다
            FileOutputStream fos = new FileOutputStream(distributionFile + File.separator + fileName + ".dcat");
            byte[] content = distributionContent.getBytes();
            fos.write(content);
            fos.flush();
            fos.close();
        }

        //데이터서비스 생성
        String dataserviceTargetPath = targetPath + File.separator + catalogDataService;

        for (DcatDataserviceInfo dcatInfo : dcatDataserviceInfoList) {
            now = LocalDateTime.now();
            LocalDateTime nowTIme = LocalDateTime.of(now.getYear(),
                    now.getMonth(), now.getDayOfMonth(), now.getHour(), now.getMinute(), now.getSecond());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            nowTIme.format(formatter);

            String fileCreateTIme = nowTIme.toString().replaceAll("[-T:]", "");

            String fileName = programId + "_" + fileCreateTIme;

            //임시로 DcatSeq로 생성 파일들 구분
            String filePath = dataserviceTargetPath + File.separator + programId;

            //파일경로 생성
            File dataserviceFile = new File(filePath);
            if (!dataserviceFile.exists()) {
                logger.info("파일경로생성");
                dataserviceFile.mkdirs();
            }

            //DCAT 파일 내용 제작
            String DataServiceContent = defaultPrefix + newLine
                    + rdfPrefix + newLine
                    + rdfsPrefix + newLine
                    + dcatPrefix + newLine
                    + dctPreFix + newLine
                    + newLine
                    + seperator + catalogDataService + newLine
                    + tab + "a dcat" + seperator + catalogDataService + space + semiCol + newLine
                    + tab + "dcat" + seperator + "endpointUrl" + space + dcatInfo.getDataserviceEndpointUrl() + space + semiCol + newLine
                    + tab + "dcat" + seperator + "endpointDescripton" + space + dcatInfo.getDataserviceEndpointDescripton() + space + semiCol + newLine
                    + tab + "dcat" + seperator + "servesDataset" + space + dcatInfo.getDataserviceServesDataset() + space + semiCol + newLine
                    + tab + terminator;
            logger.info(DataServiceContent);

            //파일 경로만 생성되었다가 파일이 생성된다
            FileOutputStream fos = new FileOutputStream(dataserviceFile + File.separator + fileName + ".dcat");
            byte[] content = DataServiceContent.getBytes();
            fos.write(content);
            fos.flush();
            fos.close();
        }
    }

    public void createDcatFileByCSV(String source, String target) throws Exception {

        File csvFile = new File(source);

        CsvMapper mapper = new CsvMapper();
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();

        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(csvSchema)
                .readValues(csvFile);

        //프리픽스 선언
        String defaultPrefix = "@prefix : <http://example.org/> .";
        String rdfPrefix = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .";
        String rdfsPrefix = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .";
        String dcatPrefix = "@prefix dcat: <http://www.w3.org/ns/dcat#> .";
        String dctPreFix = "@prefix dct: <http://purl.org/dc/terms/> .";

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

            //DCAT 파일 내용 제작
            String dcatContent =
                    defaultPrefix + newLine
                            + rdfPrefix + newLine
                            + rdfsPrefix + newLine
                            + dcatPrefix + newLine
                            + dctPreFix + newLine
                            + newLine
                            + seperator + catalogDataSet + newLine
                            + tab + "a dcat" + seperator + catalogDataSet + space + semiCol + newLine
                            + tab + "dct" + seperator + "accessRights" + space + rowAsMap.get("DATASET_accessRights") + space + semiCol + newLine
                            + tab + "dct" + seperator + "conformsTo" + space + rowAsMap.get("DATASET_conformsTo") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "contactPoint" + space + rowAsMap.get("DATASET_contactPoint") + space + semiCol + newLine
                            + tab + "dct" + seperator + "creator" + space + rowAsMap.get("DATASET_creator") + space + semiCol + newLine
                            + tab + "dct" + seperator + "description" + space + rowAsMap.get("DATASET_description") + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "title" + space + rowAsMap.get("DATASET_title") + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "issued" + space + rowAsMap.get("DATASET_issued") + space + semiCol + newLine
                            + tab + "dct" + seperator + "modified" + space + rowAsMap.get("DATASET_modified") + space + semiCol + newLine
                            + tab + "dct" + seperator + "language" + space + rowAsMap.get("DATASET_language") + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "publisher" + space + rowAsMap.get("DATASET_publisher") + space + semiCol + newLine
                            + tab + "dct" + seperator + "identifier" + space + rowAsMap.get("DATASET_identifier") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "theme" + space + rowAsMap.get("DATASET_theme") + space + semiCol + newLine
                            + tab + "dct" + seperator + "type" + space + rowAsMap.get("DATASET_type") + space + semiCol + newLine
                            + tab + "dct" + seperator + "relation" + space + rowAsMap.get("DATASET_relation") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "keyword" + space + rowAsMap.get("DATASET_keyword") + space + semiCol + newLine
                            + tab + "dct" + seperator + "license" + space + rowAsMap.get("DATASET_license") + space + semiCol + newLine
                            + tab + "dcat" + seperator + "distribution" + space + rowAsMap.get("DATASET_distribution") + space + semiCol + newLine
                            + tab + "dct" + seperator + "spatial" + space + rowAsMap.get("DATASET_spatial") + space + semiCol + newLine
                            + tab + terminator + newLine
                            + newLine
                            + seperator + catalogDistribution + newLine
                            + tab + "a dcat" + seperator + catalogDistribution + space + semiCol + newLine
                            + tab + "dct" + seperator + "title" + space + rowAsMap.get("Distribution_title") + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "description" + space + rowAsMap.get("Distribution_description") + "@ko" + space + semiCol + newLine
                            + tab + "dct" + seperator + "issued" + space + rowAsMap.get("Distribution_issued") + space + semiCol + newLine
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

            logger.info(dcatContent);

            //파일이 생성된다
            FileOutputStream dcatFos = new FileOutputStream(target);
            byte[] dcatByte = dcatContent.getBytes();
            dcatFos.write(dcatByte);
            dcatFos.flush();
            dcatFos.close();
        }
    }

    public void rdfParsing(String source, String target) throws Exception {
        File csvFile = new File(source);

        CsvMapper mapper = new CsvMapper();
        CsvSchema csvSchema = CsvSchema.emptySchema().withHeader();

        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(csvSchema)
                .readValues(csvFile);

        LocalDate currentDate = LocalDate.now();

//        String targetPath = toUploadDir + File.separator + programId;

        ModelBuilder builder = new ModelBuilder();
        builder
                .setNamespace("rdf", "http://example.org/");

        //rdf 빌더에 요소를 더하는 부분
        int i = 0;
        while (it.hasNext()) {
            i++;
            Map<String, String> rowAsMap = it.next();
            builder.subject("rdf:" + String.valueOf(i));
            logger.info(rowAsMap.keySet().toString());
            logger.info(rowAsMap.toString());
            for (String element : rowAsMap.keySet()) {
                builder.add(csvFile.getName() + ":" + element, rowAsMap.get(element).toString());
            }
        }

        Model model = builder.build();

        File targetFilePath = new File(target);

        if (!targetFilePath.exists()) {
            logger.info("파일경로생성");
            targetFilePath.mkdirs();
        }

        FileOutputStream fos = new FileOutputStream(target + ".rdf");

        Rio.write(model, fos, RDFFormat.RDFXML);
        fos.close();
    }
}