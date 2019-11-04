package com.list.fh.rdfparser.model;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class DcatDistributionInfo {
    private Long dcatSeq;
    private String distributionTitle;
    private String distributionDescription;
    private String distributionIssued;
    private String distributionLicense;
    private String distributionAccessUrl;
    private String distributionAccessService;
    private String distributionDownloadUrl;
    private String distributionSpatialResolutionlnMeters;
    private String distributionFormat;
    private String distributionCompressFormat;
    private String useYn;
    private String createdAt;
    private String modifiedAt;
}
