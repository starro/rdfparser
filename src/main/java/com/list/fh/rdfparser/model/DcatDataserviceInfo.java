package com.list.fh.rdfparser.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DcatDataserviceInfo {
    private Long dcatSeq;
    private String dataserviceEndpointUrl;
    private String dataserviceEndpointDescripton;
    private String dataserviceServesDataset;
    private String useYn;
    private String createdAt;
    private String modifiedAt;
}