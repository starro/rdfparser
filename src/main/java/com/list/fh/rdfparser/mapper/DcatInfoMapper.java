package com.list.fh.rdfparser.mapper;

import com.list.fh.rdfparser.model.DcatDataserviceInfo;
import com.list.fh.rdfparser.model.DcatDatasetInfo;
import com.list.fh.rdfparser.model.DcatDistributionInfo;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface DcatInfoMapper {
    List<DcatDatasetInfo> selectDatasetInfoList() throws Exception;
    List<DcatDistributionInfo> selectDistributionInfoList() throws Exception;
    List<DcatDataserviceInfo> selectDataserviceInfoList() throws Exception;
}
