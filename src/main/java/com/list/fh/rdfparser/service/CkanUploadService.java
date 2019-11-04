package com.list.fh.rdfparser.service;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.mime.MultipartEntityBuilder;
import org.apache.http.entity.mime.content.ContentBody;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;

@Service
public class CkanUploadService {
    static String myApiKey = "fa0499d1-ffda-4590-82b3-4afdb9c91576";
    static String uploadFileName = "/home/ilias/log.txt";

    public static String uploadFile() throws IOException {
        String myApiKey = "apikey";
        String uploadFileName = "/path/to/file.ext";
        String HOST = "http://ckan.host.com";
        String line;
        StringBuilder sb = new StringBuilder();
        CloseableHttpClient httpclient = HttpClientBuilder.create().build();
        File file = new File(uploadFileName);
        SimpleDateFormat dateFormatGmt = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String date = dateFormatGmt.format(new Date());

        HttpPost postRequest;
        file = new File(uploadFileName);
        try {

            ContentBody cbFile = new FileBody(file, ContentType.TEXT_HTML);
            MultipartEntityBuilder multipartEntityBuilder = MultipartEntityBuilder.create();
            multipartEntityBuilder.addPart("file", cbFile);
            multipartEntityBuilder.addPart("key", new StringBody(uploadFileName + date, ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("package_id", new StringBody("dataSetName", ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("url", new StringBody("path/to/save/dir", ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("upload", cbFile);
            multipartEntityBuilder.addPart("comment", new StringBody("comments", ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("notes", new StringBody("notes", ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("author", new StringBody("AuthorName", ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("author_email", new StringBody("AuthorEmail", ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("title", new StringBody("title", ContentType.TEXT_PLAIN));
            multipartEntityBuilder.addPart("description", new StringBody("file Desc" + date, ContentType.TEXT_PLAIN));
            HttpEntity reqEntity = multipartEntityBuilder
                    .build();

            postRequest = new HttpPost(HOST + "/api/3/action/resource_create");
            postRequest.setEntity(reqEntity);
            postRequest.setHeader("X-CKAN-API-Key", myApiKey);

            HttpResponse response = httpclient.execute(postRequest);
            int statusCode = response.getStatusLine().getStatusCode();
            BufferedReader br = new BufferedReader(
                    new InputStreamReader((response.getEntity().getContent())));

            sb.append(statusCode + "\n");
            if (statusCode != 200) {
                System.out.println("statusCode =!=" + statusCode);
            } else System.out.println("OK");

            while ((line = br.readLine()) != null) {
                sb.append(line + "\n");
                System.out.println("+" + line);
            }

            httpclient.close();
            return sb.toString();
        } catch (IOException ioe) {
            System.out.println(ioe);
            return "error" + ioe;
        } finally {
            httpclient.getConnectionManager().shutdown();

        }
    }
}
