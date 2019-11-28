package com.list.fh.rdfparser.service;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.validator.routines.UrlValidator;
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

    //Dcat의 Resouce 속성들 관리
    public enum DcatResouceProps {
        CONTACT_POINT("dcat:contactPoint"),
        KEYWORD("dcat:keyword"),
        LANDING_PAGE("dcat:landingPage"),
        QULIFIED_RELATION("dcat:qulifiedRelation"),
        THEME("dcat:theme"),
        ACCESS_RIGHTS("dct:accessRights"),
        CONFORMS_TO("dct:conformsTo"),
        CREATOR("dct:creator"),
        DESCRIPTION("dct:description"),
        IDENTIFIER("dct:identifier"),
        IS_REFERENCED_BY("dct:isReferencedBy"),
        ISSUED("dct:issued"),
        LANGUAGE("dct:language"),
        LICENSE("dct:license"),
        MODIFIED("dct:modified"),
        PUBLISHER("dct:publisher"),
        RELATION("dct:relation"),
        RIGHTS("dct:rights"),
        TITLE("dct:title"),
        TYPE("dct:type"),
        HAS_POLICY("odrl:hasPolicy"),
        QULIFIED_ATTRIBUTION("prov:qulifiedAttribution");
        private String prop;

        DcatResouceProps(String prop) {
            this.prop = prop;
        }

        public String prop() {
            return prop;
        }
    }

    //Dcat의 DataSet 속성들 관리
    public enum DcatDataSetProps {
        DISTRIBUTION("dcat:distribution"),
        SPATIAL_RESOLUTION_IN_METERS("dcat:spatialResolutionInMeters"),
        TEMPORAL_RESOLUTION("dcat:temporalResolution"),
        ACCRUAL_PERIODICITY("dct:accrualPeriodicity"),
        SPATIAL("dct:spatial"),
        TEMPORAL("dct:temporal");

        private String prop;

        DcatDataSetProps(String prop) {
            this.prop = prop;
        }

        public String prop() {
            return prop;
        }
    }

    //Dcat의 Distribution 속성들 관리
    public enum DcatDistributionProps {
        ACCESS_SERVICE("dcat:accessService"),
        ACCESS_URL("dcat:accessURL"),
        BYTE_SIZE("dcat:byteSize"),
        COMPRESS_FORMAT("dcat:compressFormat"),
        DOWNLOAD_URL("dcat:downloadURL"),
        MEDIA_TYPE("dcat:mediaType"),
        PACKAGE_FORMAT("dcat:packageFormat"),
        SPATIAL_RESOLUTION_IN_METERS("dcat:spatialResolutionInMeters"),
        TEMPORAL_RESOLUTION("dcat:temporalResolution"),
        ACCESS_RIGHTS("dct:accessRights"),
        DESCRIPTION("dct:description"),
        FORMAT("dct:format"),
        ISSUED("dct:issued"),
        LICENSE("dct:license"),
        MODIFIED("dct:modified"),
        RIGHTS("dct:rights"),
        TITLE("dct:title");

        private String prop;

        DcatDistributionProps(String prop) {
            this.prop = prop;
        }

        public String prop() {
            return prop;
        }
    }

    //Dcat의 DataService 속성들 관리
    public enum DcatDataServiceProps {
        ENDPOINT_URL("dcat:endpointURL"),
        ENDPOINT_DESCRIPTION("dcat:endpointDescription"),
        SERVES_DATASET("dcat:servesDataset");

        private String prop;

        DcatDataServiceProps(String prop) {
            this.prop = prop;
        }

        public String prop() {
            return prop;
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
        String rdfPrefix = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .";
        String rdfsPrefix = "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .";
        String dcatPrefix = "@prefix dcat: <http://www.w3.org/ns/dcat#> .";
        String dctPreFix = "@prefix dct: <http://purl.org/dc/terms/> .";
        String foafPreFix = "@prefix foaf: <http://xmlns.com/foaf/0.1/> .";

        //DATASET 제작 시작
        //dcat내용 제작을 위한 변수들
        String dcatDataSet = "DATASET";
        String dcatDistribution = "Distribution";
        String dcatDataService = "DataService";

        //dcat 표현을 위한 기타 문자
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

            //데이터셋의 키워드 가공
            //키워드는 쉼표로 분리한다.
            String dcatDatasetKeywords = "";
            if (StringUtils.isNotEmpty(rowAsMap.get("DATASET_keyword"))) {
                String datasetKeywords = rowAsMap.get("DATASET_keyword");
                String[] dsKeywords = datasetKeywords.split(",");

                for (String modString : dsKeywords) {
                    dcatDatasetKeywords = dcatDatasetKeywords + "\"" + modString.trim() + "\"" + "@" + rowAsMap.get("DATASET_language") + space;
                }

                //키워드를 분리시켜서 더블쿼테이션과 랭귀지를 단다.
                dcatDatasetKeywords = dcatDatasetKeywords.replaceAll(" ", ", ");
                dcatDatasetKeywords = dcatDatasetKeywords.trim();
                dcatDatasetKeywords = dcatDatasetKeywords.substring(0, dcatDatasetKeywords.length() - 1);
            }

            //데이터서비스의 키워드 가공
            //키워드는 쉼표로 분리한다.
            String dcatDataServiceKeywords = "";
            if (StringUtils.isNotEmpty(rowAsMap.get("DataService_keyword"))) {
                String dataServiceKeywords = rowAsMap.get("DataService_keyword");
                String[] dServiceKeywords = dataServiceKeywords.split(",");
                for (String modString : dServiceKeywords) {
                    dcatDataServiceKeywords = dcatDataServiceKeywords + "\"" + modString.trim() + "\"" + "@" + rowAsMap.get("DataService_language") + space;
                }

                //키워드를 분리시켜서 더블쿼테이션과 랭귀지를 단다.
                dcatDataServiceKeywords = dcatDataServiceKeywords.replaceAll(" ", ", ");
                dcatDataServiceKeywords = dcatDataServiceKeywords.trim();
                dcatDataServiceKeywords = dcatDataServiceKeywords.substring(0, dcatDataServiceKeywords.length() - 1);
            }
            //값들 중에 Url이 들어있는지 파악하기 위해


            //DCAT 파일 내용 제작
            //DCAT의 DataSet과 DataService 속성의 일부는 DCAT의 Resource를 상속 받는다
            String dcatContent = rdfPrefix + newLine
                    + rdfsPrefix + newLine
                    + dcatPrefix + newLine
                    + dctPreFix + newLine
                    + foafPreFix + newLine
                    + newLine
                    + seperator + dcatDataSet + newLine
                    + tab + "a dcat" + seperator + dcatDataSet + space + semiCol + newLine;
            //Resource 속성

            dcatContent = dcatContent + tab + DcatResouceProps.ACCESS_RIGHTS.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_accessRights")) + space + semiCol + newLine;
            dcatContent = dcatContent + tab + DcatResouceProps.CONTACT_POINT.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_contactPoint")) + space + semiCol + newLine;
            dcatContent = dcatContent + tab + DcatResouceProps.CREATOR.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_creator")) + space + semiCol + newLine;
            dcatContent = dcatContent + tab + DcatResouceProps.DESCRIPTION.prop() + space + "\"" + rowAsMap.get("DATASET_description") + "\"" + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine
                    + tab + DcatResouceProps.TITLE.prop() + space + "\"" + rowAsMap.get("DATASET_title") + "\"" + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine
                    + tab + DcatResouceProps.ISSUED.prop() + space + "\"" + rowAsMap.get("DATASET_issued") + "\"^^xsd:date" + space + semiCol + newLine
                    + tab + DcatResouceProps.MODIFIED.prop() + space + "\"" + rowAsMap.get("DATASET_modified") + "\"^^xsd:date" + space + semiCol + newLine
                    + tab + DcatResouceProps.LANGUAGE.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_language")) + space + semiCol + newLine
                    + tab + DcatResouceProps.PUBLISHER.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_publisher")) + space + semiCol + newLine
                    + tab + DcatResouceProps.IDENTIFIER.prop() + space + "\"" + rowAsMap.get("DATASET_identifier") + "\"^^xsd:string" + space + semiCol + newLine
                    + tab + DcatResouceProps.THEME.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_theme")) + space + semiCol + newLine
                    + tab + DcatResouceProps.TYPE.prop() + space + rowAsMap.get("DATASET_type") + space + semiCol + newLine;
            //선택 가능 필드(KEYWORD)
            if (StringUtils.isNotEmpty(rowAsMap.get("DATASET_keyword"))) {
                dcatContent = dcatContent + tab + DcatResouceProps.KEYWORD.prop() + space + dcatDatasetKeywords + space + semiCol + newLine;
            }
            //필수
            dcatContent = dcatContent + tab + DcatResouceProps.LANDING_PAGE.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_landingPage")) + space + semiCol + newLine
                    + tab + DcatResouceProps.LICENSE.prop() + space + urlTypeConverter(rowAsMap.get("DATASET_license")) + space + semiCol + newLine
                    + tab + DcatResouceProps.RIGHTS.prop() + space + rowAsMap.get("DATASET_rights") + space + semiCol + newLine
                    //여기서부터 DATASET의 고유속성
                    + tab + DcatDataSetProps.DISTRIBUTION.prop() + space + rowAsMap.get("DATASET_distribution") + space + semiCol + newLine;

            //선택 가능 필드(SPATIAL_RESOLUTION_IN_METERS, TEMPORAL_RESOLUTION, ACCRUAL_PERIODICITY, SPATIAL, TEMPORAL)
            if (StringUtils.isNotEmpty(rowAsMap.get("DATASET_spatialResolutionInMeters"))) {
                dcatContent = dcatContent + tab + DcatDataSetProps.SPATIAL_RESOLUTION_IN_METERS.prop() + space + rowAsMap.get("DATASET_spatialResolutionInMeters") + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("DATASET_temporalResolution"))) {
                dcatContent = dcatContent + tab + DcatDataSetProps.TEMPORAL_RESOLUTION.prop() + space + "\"" + rowAsMap.get("DATASET_temporalResolution") + "\"^^xsd:duration" + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("DATASET_accrualPeriodicity"))) {
                dcatContent = dcatContent + tab + DcatDataSetProps.ACCRUAL_PERIODICITY.prop() + space + rowAsMap.get("DATASET_accrualPeriodicity") + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("DATASET_spatial"))) {
                dcatContent = dcatContent + tab + DcatDataSetProps.SPATIAL.prop() + space + rowAsMap.get("DATASET_spatial") + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("DATASET_temporal"))) {
                dcatContent = dcatContent + tab + DcatDataSetProps.TEMPORAL.prop() + space + rowAsMap.get("DATASET_temporal") + space + semiCol + newLine;
            }
            dcatContent = dcatContent + tab + terminator + newLine + newLine;

            //Distribution 시작
            dcatContent = dcatContent + seperator + dcatDistribution + newLine
                    + tab + "a dcat" + seperator + dcatDistribution + space + semiCol + newLine;
            //선택 가능 필드(ACCESS_SERVICE, ACCESS_URL)
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_accessService"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.ACCESS_SERVICE.prop() + space + rowAsMap.get("Distribution_accessService") + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_accessURL"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.ACCESS_URL.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_accessURL")) + space + semiCol + newLine;
            }
            //필수
            dcatContent = dcatContent + tab + DcatDistributionProps.BYTE_SIZE.prop() + space + "\"" + rowAsMap.get("Distribution_byteSize") + "\"^^xsd:decimal" + space + semiCol + newLine;
            //선택 가능 필드(COMPRESS_FORMAT, DOWNLOAD_URL, MEDIA_TYPE, PACKAGE_FORMAT, SPATIAL_RESOLUTION_IN_METERS, TEMPORAL_RESOLUTION, ACCESS_RIGHTS, DESCRIPTION)
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_compressFormat"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.COMPRESS_FORMAT.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_compressFormat")) + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_downloadURL"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.DOWNLOAD_URL.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_downloadURL")) + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_mediaType"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.MEDIA_TYPE.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_mediaType")) + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_packageFormat"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.PACKAGE_FORMAT.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_packageFormat")) + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_spatialResolutionInMeters"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.SPATIAL_RESOLUTION_IN_METERS.prop() + space + "\"" + rowAsMap.get("Distribution_spatialResolutionInMeters") + "\"^^xsd:decimal" + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_temporalResolution"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.TEMPORAL_RESOLUTION.prop() + space + "\"" + rowAsMap.get("Distribution_temporalResolution") + "\"^^xsd:duration" + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_accessRights"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.ACCESS_RIGHTS.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_accessRights")) + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_description"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.DESCRIPTION.prop() + space + "\"" + rowAsMap.get("Distribution_description") + "\"" + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine;
            }
            //필수
            dcatContent = dcatContent + tab + DcatDistributionProps.FORMAT.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_format")) + space + semiCol + newLine
                    + tab + DcatDistributionProps.ISSUED.prop() + space + "\"" + rowAsMap.get("Distribution_issued") + "\"^^xsd:date" + space + semiCol + newLine
                    + tab + DcatDistributionProps.LICENSE.prop() + space + urlTypeConverter(rowAsMap.get("Distribution_license")) + space + semiCol + newLine
                    + tab + DcatDistributionProps.MODIFIED.prop() + space + "\"" + rowAsMap.get("Distribution_modified") + "\"^^xsd:date" + space + semiCol + newLine;
            //선택 가능 필드(RIGHTS)
            if (StringUtils.isNotEmpty(rowAsMap.get("Distribution_rights"))) {
                dcatContent = dcatContent + tab + DcatDistributionProps.RIGHTS.prop() + space + rowAsMap.get("Distribution_rights") + space + semiCol + newLine;
            }
            //필수
            dcatContent = dcatContent + tab + DcatDistributionProps.TITLE.prop() + space + "\"" + rowAsMap.get("Distribution_title") + "@" + rowAsMap.get("DATASET_language") + space + semiCol + newLine;

            //DataService 시작
            dcatContent = dcatContent + tab + terminator + newLine
                    + newLine
                    + seperator + dcatDataService + newLine
                    + tab + "a dcat" + seperator + dcatDataService + space + semiCol + newLine
                    //Resource 속성
                    + tab + DcatResouceProps.ACCESS_RIGHTS.prop() + space + urlTypeConverter(rowAsMap.get("DataService_accessRights")) + space + semiCol + newLine
                    + tab + DcatResouceProps.CONTACT_POINT.prop() + space + urlTypeConverter(rowAsMap.get("DataService_contactPoint")) + space + semiCol + newLine
                    + tab + DcatResouceProps.CREATOR.prop() + space + urlTypeConverter(rowAsMap.get("DataService_creator")) + space + semiCol + newLine
                    + tab + DcatResouceProps.DESCRIPTION.prop() + space + "\"" + rowAsMap.get("DataService_description") + "\"" + "@" + rowAsMap.get("DataService_language") + space + semiCol + newLine
                    + tab + DcatResouceProps.TITLE.prop() + space + "\"" + rowAsMap.get("DataService_title") + "\"" + "@" + rowAsMap.get("DataService_language") + space + semiCol + newLine
                    + tab + DcatResouceProps.ISSUED.prop() + space + "\"" + rowAsMap.get("DataService_issued") + "\"^^xsd:date" + space + semiCol + newLine
                    + tab + DcatResouceProps.MODIFIED.prop() + space + "\"" + rowAsMap.get("DataService_modified") + "\"^^xsd:date" + space + semiCol + newLine
                    + tab + DcatResouceProps.LANGUAGE.prop() + space + urlTypeConverter(rowAsMap.get("DataService_language")) + space + semiCol + newLine
                    + tab + DcatResouceProps.PUBLISHER.prop() + space + urlTypeConverter(rowAsMap.get("DataService_publisher")) + space + semiCol + newLine
                    + tab + DcatResouceProps.IDENTIFIER.prop() + space + "\"" + rowAsMap.get("DataService_identifier") + "\"^^xsd:string" + space + semiCol + newLine
                    + tab + DcatResouceProps.THEME.prop() + space + urlTypeConverter(rowAsMap.get("DataService_theme")) + space + semiCol + newLine;
            //선택 가능 필드(TYPE, KEYWORD, LANDING_PAGE)
            if (StringUtils.isNotEmpty(rowAsMap.get("DataService_type"))) {
                dcatContent = dcatContent + tab + DcatResouceProps.TYPE.prop() + space + rowAsMap.get("DataService_type") + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("DataService_keyword"))) {
                dcatContent = dcatContent + tab + DcatResouceProps.KEYWORD.prop() + space + dcatDataServiceKeywords + space + semiCol + newLine;
            }
            if (StringUtils.isNotEmpty(rowAsMap.get("DataService_landingPage"))) {
                dcatContent = dcatContent + tab + DcatResouceProps.LANDING_PAGE.prop() + space + urlTypeConverter(rowAsMap.get("DataService_landingPage")) + space + semiCol + newLine;
            }
            //필수
            dcatContent = dcatContent + tab + DcatResouceProps.LICENSE.prop() + space + urlTypeConverter(rowAsMap.get("DataService_license")) + space + semiCol + newLine
                    + tab + DcatResouceProps.RIGHTS.prop() + space + rowAsMap.get("DataService_rights") + space + semiCol + newLine
                    //DataService 고유 속성
                    + tab + DcatDataServiceProps.ENDPOINT_URL.prop() + space + urlTypeConverter(rowAsMap.get("DataService_endpointURL")) + space + semiCol + newLine
                    + tab + DcatDataServiceProps.ENDPOINT_DESCRIPTION.prop() + space + "\"" + rowAsMap.get("DataService_endpointDescripton") + "\"" + "@" + rowAsMap.get("DataService_language") + space + semiCol + newLine;
            //선택 가능 필드(SERVES_DATASET)
            if (StringUtils.isNotEmpty(rowAsMap.get("DataService_servesDataset"))) {
                dcatContent = dcatContent + tab + DcatDataServiceProps.SERVES_DATASET.prop() + space + rowAsMap.get("DataService_servesDataset") + space + semiCol + newLine;
            }
            dcatContent = dcatContent + tab + terminator;

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

    public String urlTypeConverter(String val) {
        UrlValidator urlValidator = new UrlValidator();
        String validVal = "";
        if (urlValidator.isValid(val)) {
            validVal = "<" + val + ">";
        } else {
            validVal = val;
        }
        return validVal;
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