package com.list.fh.rdfparser.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DcatDatasetInfo {
    private Long dcatSeq;
    private String datasetAccessRights;
    private String datasetConformsTo;
    private String datasetContactPoint;
    private String datasetCreator;
    private String datasetDescription;
    private String datasetTitle;
    private String datasetIssued;
    private String datasetModified;
    private String datasetLanguage;
    private String datasetPublisher;
    private String datasetIdentifier;
    private String datasetTheme;
    private String datasetType;
    private String datasetRelation;
    private String datasetKeyword;
    private String datasetLicense;
    private String datasetDistribution;
    private String datasetSpatial;
    private String useYn;
    private String createdAt;
    private String modifiedAt;
}
