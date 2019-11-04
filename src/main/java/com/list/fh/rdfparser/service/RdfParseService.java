package com.list.fh.rdfparser.service;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvParser;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.list.fh.rdfparser.mapper.DcatInfoMapper;
import com.list.fh.rdfparser.model.DcatDataserviceInfo;
import com.list.fh.rdfparser.model.DcatDatasetInfo;
import com.list.fh.rdfparser.model.DcatDistributionInfo;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

@Service
public class RdfParseService {

    @Autowired
    private DcatInfoMapper mapper;

    public void processExecute(String id, String targetDir) {
        System.out.println("id: " + id + " targetDir: " + targetDir);

        //원본 csv read 과정
//        try {
////            this.readCSV(id);
//            this.createDcat();
//            this.createRdf();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        //dcat 생성과정

        //rdf 생성과정

    }

    //csv 읽기
    private void readCSV() throws IOException {
        File csvFile = new File("D:\\test.csv");
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema1 = CsvSchema.emptySchema().withHeader();
        System.out.println("스키마");
        System.out.println(schema1);
        System.out.println("스키마");
        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(schema1)
                .readValues(csvFile);
        while (it.hasNext()) {
            Map<String, String> rowAsMap = it.next();
            // access by column name, as defined in the header row...
            System.out.println(rowAsMap);
        }

        CsvMapper mapper2 = new CsvMapper();

        mapper2.enable(CsvParser.Feature.WRAP_AS_ARRAY);
        File csvFile2 = new File("D:\\test.csv");
        MappingIterator<String[]> it2 = mapper2.readerFor(String[].class).readValues(csvFile2);
        while (it2.hasNext()) {
            ;
            // and voila, column values in an array. Works with Lists as well
            System.out.println(it2.next().toString());
        }
    }

    public void readCSV(String source) throws IOException {
        File csvFile = new File(source);
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema1 = CsvSchema.emptySchema().withHeader();

        System.out.println(schema1);

        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(schema1)
                .readValues(csvFile);

        while (it.hasNext()) {
            Map<String, String> rowAsMap = it.next();

            System.out.println(rowAsMap.keySet().toString());
            // access by column name, as defined in the header row...
            System.out.println(rowAsMap);
        }
    }

    public void rdfParsing() throws IOException {
        String path = "D:";
        String seperator = "\\";
        String programId = "nia1";
        LocalDateTime currentDateTime = LocalDateTime.now();

        String targetPath = path + seperator + programId + seperator + currentDateTime.toString() + seperator;

        ValueFactory vf = SimpleValueFactory.getInstance();
        BNode address = vf.createBNode();

// First we do the same thing we did in example 02: create a new ModelBuilder,
// and add two statements about Picasso.
        ModelBuilder builder = new ModelBuilder();
        builder
                .setNamespace("catalog", "http://example.org/")
                .subject("catalog:Picasso")
                .add(RDF.TYPE, "dct:Test")
                .add("dct:creator", "상훈")
                .add("dcat:Distribution", ":abc");
        // Bnode 삽입
//                .add("ex:homeAddress", address) // link the blank node
//                .subject(address)			    // switch the subject
//                .add("ex:street", "31 Art Gallery")
//                .add("ex:city", "Madrid")
//                .add("ex:country", "Spain");

        Model model = builder.build();

        File targetFilePath = new File(targetPath);

        if (!targetFilePath.exists()) {
            System.out.println("파일경로생성");
            targetFilePath.mkdirs();
        }

        FileOutputStream fos = new FileOutputStream(targetFilePath + seperator + "test.rdf");

        Rio.write(model, fos, RDFFormat.TURTLE);
        fos.close();
    }

    public void rdfParsing(String source, String target) throws IOException {
        File csvFile = new File(source);
        CsvMapper mapper = new CsvMapper();
        CsvSchema schema1 = CsvSchema.emptySchema().withHeader();

        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(schema1)
                .readValues(csvFile);

        String path = "D:";
        String seperator = "\\";
        String programId = "nia1";
        LocalDate currentDate = LocalDate.now();

        String targetPath = path + seperator + programId + seperator + currentDate.toString() + seperator;

        ModelBuilder builder = new ModelBuilder();
        builder
                .setNamespace("catalog", "http://purl.org/dc/terms/")
                .subject("a dcat:Catalog")
                .add("dct:title", "Imaginary Catalog")
                .add("dct:dataset", "dataset-001");

        //rdf 빌더에 요소를 더하는 부분
        while (it.hasNext()) {
//            Map<String, String> rowAsMap = it.next();
//
//            System.out.println(rowAsMap.keySet().toString());
//
//            builder.subject("dcat:Distribution")
//                    .add("dcat:distribution", rowAsMap.get("Dcat_seq ").toString());
//            System.out.println(rowAsMap.get("Dcat_seq ").toString());
//            System.out.println(rowAsMap);
        }

        Model model = builder.build();

        File targetFilePath = new File(targetPath);

        if (!targetFilePath.exists()) {
            System.out.println("파일경로생성");
            targetFilePath.mkdirs();
        }

        FileOutputStream fos = new FileOutputStream(targetFilePath + seperator + "test2.rdf");

        Rio.write(model, fos, RDFFormat.RDFXML);
        fos.close();
    }

    public void createDcat(String name) throws Exception {

        List<DcatDatasetInfo> dcatInfoList = mapper.selectDatasetInfoList();
        List<DcatDistributionInfo> dcatDistributionInfoList = mapper.selectDistributionInfoList();
        List<DcatDataserviceInfo> dcatDataserviceInfoList = mapper.selectDataserviceInfoList();

        //파일명제작
        String path = "D:";
        String pathSeperator = "\\";
        String programId = name;


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
        for (DcatDatasetInfo dcatInfo : dcatInfoList) {
            now = LocalDateTime.now();
            LocalDateTime nowTIme = LocalDateTime.of(now.getYear(),
                    now.getMonth(), now.getDayOfMonth(), now.getHour(), now.getMinute(), now.getSecond());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            nowTIme.format(formatter);

            String fileCreateTIme = nowTIme.toString().replaceAll("[-T:]", "");

            String targetPath = path + pathSeperator + programId + pathSeperator + dcatInfo.getDcatSeq() + pathSeperator + programId;
            String fileName = programId + "_" + fileCreateTIme;

            File datasetFile = new File(targetPath);

            if (!datasetFile.exists()) {
                System.out.println("파일경로생성");
                datasetFile.mkdirs();
            }

            String datasetContent = seperator + catalogDataSet + newLine
                    + tab + "a dcat" + seperator + catalogDataSet + space + semiCol + newLine
                    + tab + "dct" + seperator + "accessRights" + space + dcatInfo.getDatasetAccessRights() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "conformsTo" + space + dcatInfo.getDatasetConformsTo() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "contactPoint" + space + dcatInfo.getDatasetContactPoint() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "creator" + space + dcatInfo.getDatasetCreator() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "description" + space + dcatInfo.getDatasetDescription() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "title" + space + dcatInfo.getDatasetTitle() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "issued" + space + dcatInfo.getDatasetIssued() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "modified" + space + dcatInfo.getDatasetModified() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "language" + space + dcatInfo.getDatasetLanguage() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "publisher" + space + dcatInfo.getDatasetPublisher() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "identifier" + space + dcatInfo.getDatasetIdentifier() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "theme" + space + dcatInfo.getDatasetTheme() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "type" + space + dcatInfo.getDatasetType() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "relation" + space + dcatInfo.getDatasetRelation() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "keyword" + space + dcatInfo.getDatasetKeyword() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "license" + space + dcatInfo.getDatasetLicense() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "distribution" + space + dcatInfo.getDatasetDistribution() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "spatial" + space + dcatInfo.getDatasetSpatial() + "@ko" + space + semiCol + newLine
                    + tab + terminator;

            System.out.println(datasetContent);

            //파일 경로만 생성되었다가 파일이 생성된다
            FileOutputStream fos = new FileOutputStream(datasetFile + File.separator + fileName + ".rdf");
            byte[] content = datasetContent.getBytes();
            fos.write(content);
            fos.flush();
            fos.close();
        }


        for (DcatDistributionInfo dcatInfo : dcatDistributionInfoList) {
            now = LocalDateTime.now();
            LocalDateTime nowTIme = LocalDateTime.of(now.getYear(),
                    now.getMonth(), now.getDayOfMonth(), now.getHour(), now.getMinute(), now.getSecond());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            nowTIme.format(formatter);

            String fileCreateTIme = nowTIme.toString().replaceAll("[-T:]", "");

            String targetPath = path + pathSeperator + programId + pathSeperator + dcatInfo.getDcatSeq() + pathSeperator + programId;
            String fileName = programId + "_" + fileCreateTIme;

            File distributionFile = new File(targetPath);

            if (!distributionFile.exists()) {
                System.out.println("파일경로생성");
                distributionFile.mkdirs();
            }
            System.out.println(distributionFile.createNewFile());

            String distributionContent = seperator + catalogDistribution + newLine
                    + tab + "a dcat" + seperator + catalogDistribution + space + semiCol + newLine
                    + tab + "dct" + seperator + "title" + space + dcatInfo.getDistributionTitle() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "description" + space + dcatInfo.getDistributionDescription() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "issued" + space + dcatInfo.getDistributionIssued() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "license" + space + dcatInfo.getDistributionLicense() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "accessUrl" + space + dcatInfo.getDistributionAccessUrl() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "accessService" + space + dcatInfo.getDistributionAccessService() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "downloadUrl" + space + dcatInfo.getDistributionDownloadUrl() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "spatialResolutionlnMeters" + space + dcatInfo.getDistributionSpatialResolutionlnMeters() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "format" + space + dcatInfo.getDistributionFormat() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "compressFormat" + space + dcatInfo.getDistributionCompressFormat() + "@ko" + space + semiCol + newLine
                    + tab + terminator;

            System.out.println(distributionContent);

            //파일 경로만 생성되었다가 파일이 생성된다
            FileOutputStream fos = new FileOutputStream(distributionFile + File.separator + fileName + ".rdf");
            byte[] content = distributionContent.getBytes();
            fos.write(content);
            fos.flush();
            fos.close();
        }

        for (DcatDataserviceInfo dcatInfo : dcatDataserviceInfoList) {
            now = LocalDateTime.now();
            LocalDateTime nowTIme = LocalDateTime.of(now.getYear(),
                    now.getMonth(), now.getDayOfMonth(), now.getHour(), now.getMinute(), now.getSecond());
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            nowTIme.format(formatter);

            String fileCreateTIme = nowTIme.toString().replaceAll("[-T:]", "");

            String targetPath = path + pathSeperator + programId + pathSeperator + dcatInfo.getDcatSeq() + pathSeperator + programId;
            String fileName = programId + "_" + fileCreateTIme;

            File dataserviceFile = new File(targetPath);
            if (!dataserviceFile.exists()) {
                System.out.println("파일경로생성");
                dataserviceFile.mkdirs();
            }

            String DataServiceContent = seperator + catalogDataService + newLine
                    + "a dcat" + seperator + catalogDataService + space + semiCol + newLine
                    + tab + "dct" + seperator + "endpointUrl" + space + dcatInfo.getDataserviceEndpointUrl() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "endpointDescripton" + space + dcatInfo.getDataserviceEndpointDescripton() + "@ko" + space + semiCol + newLine
                    + tab + "dct" + seperator + "servesDataset" + space + dcatInfo.getDataserviceServesDataset() + "@ko" + space + semiCol + newLine
                    + tab + terminator;
            System.out.println(DataServiceContent);

            //파일 경로만 생성되었다가 파일이 생성된다

            FileOutputStream fos = new FileOutputStream(dataserviceFile + File.separator + fileName + ".rdf");
            byte[] content = DataServiceContent.getBytes();
            fos.write(content);
            fos.flush();
            fos.close();
        }
    }
}