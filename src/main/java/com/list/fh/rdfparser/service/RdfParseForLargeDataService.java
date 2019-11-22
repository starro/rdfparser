package com.list.fh.rdfparser.service;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.input.ReversedLinesFileReader;
import org.eclipse.rdf4j.common.xml.XMLUtil;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.util.ModelBuilder;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
public class RdfParseForLargeDataService {

    public static final Logger logger = LoggerFactory.getLogger(RdfParseForLargeDataService.class);

    public void rdfParsing(String source, String target, String url) throws Exception {
        File csvFile = new File(source);

        CsvMapper mapper = new CsvMapper();
        CsvSchema csvSchema = mapper.schemaWithHeader().withNullValue("None");

        MappingIterator<Map<String, String>> it = mapper.readerFor(Map.class)
                .with(csvSchema)
                .readValues(csvFile);

        LocalDate currentDate = LocalDate.now();

        String file_path = target;
        int lastIndex = file_path.lastIndexOf(File.separator);
        String filePath = file_path.substring(0, lastIndex + 1);

        String sourcePath = source;

        int sourceIndex = sourcePath.lastIndexOf(File.separator);
        String sourceFileName = sourcePath.substring(sourceIndex + 1);

        String title = sourceFileName.replaceAll(".csv", "");

        String defaultNameSpace = "dataset";

        ModelBuilder builder = new ModelBuilder();
        builder
                .setNamespace(defaultNameSpace, url)
                .setNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");

        Model model = builder.build();

        File targetPath = new File(filePath);
        if (!targetPath.exists()) {
            targetPath.mkdirs();
        }

        try (FileOutputStream fos = new FileOutputStream(target)) {
            Rio.write(model, fos, RDFFormat.RDFXML);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //파일의 마지막 라인에 접근할 수 있다.
        List<String> lastLine = readLastLine(new File(target), 1);

        //RDF xml 형식으로 만들어진 파일을 다시 열어서 마지막 라인만 제거한다.
        File targetFile = new File(target);

        List<String> lines = FileUtils.readLines(targetFile, StandardCharsets.UTF_8);
        List<String> updatedLines = lines.stream().filter(s -> !s.contains(lastLine.get(0))).collect(Collectors.toList());
        FileUtils.writeLines(targetFile, updatedLines, false);

        //전체 RDF 파일의 마지막 라인
        StringBuffer endOfRdfTag = new StringBuffer();
        endOfRdfTag.append(lastLine.get(0));

        //RDF XML 파일 내용 채우기 시작
        //subject 정의
        StringBuffer subjectTagStart = new StringBuffer();
        subjectTagStart.append("<rdf:Description rdf:about=\"");
        StringBuffer amp = new StringBuffer();
        amp.append("&amp;");
        StringBuffer subjectTagEnd = new StringBuffer();
        subjectTagEnd.append("\">\n");

        StringBuffer subjectTagEndOfSet = new StringBuffer();
        subjectTagEndOfSet.append("</rdf:Description>\n");


        //subject안에 내용 정의
        StringBuffer bufferTab = new StringBuffer();
        bufferTab.append("\t");

        StringBuffer contentTagStartStart = new StringBuffer();
        contentTagStartStart.append(bufferTab).append("<").append(defaultNameSpace).append(":");
        StringBuffer contentTagStartEnd = new StringBuffer();
        contentTagStartEnd.append(">");

        StringBuffer contentTagEndStart = new StringBuffer();
        contentTagEndStart.append("</dataset:");
        StringBuffer contentTagEndEnd = new StringBuffer();
        contentTagEndEnd.append(">\n");

        StringBuffer contentTagSet = new StringBuffer();

        StringBuffer subjectTagStartOfSet = new StringBuffer();

        try (BufferedWriter fw = new BufferedWriter(new FileWriter(targetFile, true))) {
            //수행시간 측정을 위한 시작 시간
            long beforeTime = System.currentTimeMillis(); //코드 실행 전에 시간 받아오기

            //Rdf Description 을 행만큼 생성하기 위한 index이다
            long longIndex = 0;
            while (it.hasNextValue()) {
                longIndex++;
                Map<String, String> rowAsMap = it.next();

                subjectTagStartOfSet.append(subjectTagStart).append(url).append("dataset?q=").append(title).append(amp).append(String.valueOf(longIndex)).append(subjectTagEnd);

                //데이터 내용 중 xml escape character가 포함되면 치환한다.

                for (String element : rowAsMap.keySet()) {
                    contentTagSet.append(contentTagStartStart).append(element).append(contentTagStartEnd).append(XMLUtil.escapeCharacterData(rowAsMap.get(element))).append(contentTagEndStart).append(element).append(contentTagEndEnd);
                }

                subjectTagStartOfSet.append(contentTagSet);

                subjectTagStartOfSet.append(subjectTagEndOfSet);
                fw.write(subjectTagStartOfSet.toString());
                fw.flush();
                contentTagSet.setLength(0);
                subjectTagStartOfSet.delete(0, subjectTagStartOfSet.length() - 1);
            }

            fw.write("\n");
            fw.write(endOfRdfTag.toString());
            fw.flush();

            //쓰기 시간 측정
            long afterTime = System.currentTimeMillis(); // 코드 실행 후에 시간 받아오기
            long secDiffTime = (afterTime - beforeTime) / 1000; //두 시간에 차 계산

            System.out.println("Start Time : " + beforeTime);
            System.out.println("End   Time : " + afterTime);
            System.out.println("Diff   Sec : " + secDiffTime);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static List<String> readLastLine(File file, int numLastLineToRead) {

        List<String> result = new ArrayList<>();

        try (ReversedLinesFileReader reader = new ReversedLinesFileReader(file, StandardCharsets.UTF_8)) {

            String line = "";
            while ((line = reader.readLine()) != null && result.size() < numLastLineToRead) {
                result.add(line);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return result;

    }

}