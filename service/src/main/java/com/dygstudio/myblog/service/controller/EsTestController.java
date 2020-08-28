package com.dygstudio.myblog.service.controller;

import com.dygstudio.myblog.service.common.EsDocument;
import com.dygstudio.myblog.service.common.EsIndex;
import com.dygstudio.myblog.service.common.EsUtil;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 〈功能概述〉
 *
 * @className: esTestController
 * @package: com.dygstudio.myblog.service.controller
 * @author: diyaguang
 * @date: 2020/8/21 10:08 上午
 */
@RestController
@RequestMapping("/api/es")
class esTestController {


    private EsUtil esUtil = new EsUtil();

    @RequestMapping("/init")
    public String initElasticSearch(){
        esUtil.initEs();
        return "Init ElasticSearch Over!";
    }

    @RequestMapping("/baseRequest")
    public String executeRequest(){
        esUtil.initEs();
        String result = esUtil.executeRequest();
        return result;
    }
    @RequestMapping("/baseAsyncRequest")
    public String executeAsyncRequest(){
        esUtil.initEs();
        String result = esUtil.executeRequestAsync();
        return result;
    }

    @RequestMapping("/ai")
    public String executeAnalyzeIndex(String text){
        if(Strings.isNullOrEmpty(text)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        esIndex.executeAnalyzeRequest(text,esUtil);
        return "Execute IndexRequest success!";
    }
    @RequestMapping("/ci")
    public String executeCreateIndex(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        esIndex.executeCreateIndexRequest(indexName,esUtil);
        return "Execute CreateIndexRequest success!";
    }
    @RequestMapping("/gi")
    public String executeGetIndex(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        esIndex.executeGetIndexRequest(indexName,esUtil);
        return "Execute GetIndexRequest success!";
    }
    @RequestMapping("/di")
    public String executeDeleteIndex(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        esIndex.executeDeleteIndexRequest(indexName,esUtil);
        return "Execute deleteIndexRequest success!";
    }

    @RequestMapping("/ei")
    public String executeExistsIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        esIndex.executeExistsIndexRequest(indexName,esUtil);
        return "Execute existsIndexRequest success!";
    }
    @RequestMapping("/oi")
    public String executeOpenIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeOpenIndexRequest(indexName,esUtil);
    }
    @RequestMapping("/cli")
    public String executeCloseIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeCloseIndexRequest(indexName,esUtil);
    }
    @RequestMapping("/ri")
    public String executeResizeIndexRequest(String sourceIndexName,String targetIndexName){
        if(Strings.isNullOrEmpty(sourceIndexName) || Strings.isNullOrEmpty(targetIndexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeResizeRequest(sourceIndexName,targetIndexName,esUtil);
    }
    @RequestMapping("/si")
    public String executeSplitIndexRequest(String sourceIndexName,String targetIndexName){
        if(Strings.isNullOrEmpty(sourceIndexName) || Strings.isNullOrEmpty(targetIndexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeSplitRequest(sourceIndexName,targetIndexName,esUtil);
    }
    @RequestMapping("/rri")
    public String executeRefreshIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeRefreshRequest(indexName,esUtil);
    }
    @RequestMapping("/fi")
    public String executeFlushIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeFlushRequest(indexName,esUtil);
    }
    @RequestMapping("/sfi")
    public String executeSyncFlushIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeSyncFlushRequest(indexName,esUtil);
    }
    @RequestMapping("/cic")
    public String executeClearIndexCacheRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeSyncFlushRequest(indexName,esUtil);
    }
    @RequestMapping("/fmi")
    public String executeForceMergeIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeForceMergeIndexRequest(indexName,esUtil);
    }
    @RequestMapping("/rli")
    public String executeRolloverIndexRequest(String indexName){
        if(Strings.isNullOrEmpty(indexName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeRolloverIndexRequest(indexName,esUtil);
    }
    @RequestMapping("/ia")
    public String executeIndicesAliasesRequest(String indexName,String indexAliasName){
        if(Strings.isNullOrEmpty(indexName) || Strings.isNullOrEmpty(indexAliasName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeIndicesAliasesRequest(indexName,indexAliasName,esUtil);
    }
    @RequestMapping("/ga")
    public String executeGetAliasesRequest(String indexAliasName){
        if(Strings.isNullOrEmpty(indexAliasName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeGetAliasesRequest(indexAliasName,esUtil);
    }

    @RequestMapping("/gaa")
    public String executeGetAliasesRequestForAliases(String indexAliasName){
        if(Strings.isNullOrEmpty(indexAliasName)){
            return "Parameters are wrong!";
        }
        EsIndex esIndex = new EsIndex();
        return esIndex.executeGetAliasesRequestForAliases(indexAliasName,esUtil);
    }
    @RequestMapping("/dir")
    public String executeIndexRequest(String indexName,String document){
        if(Strings.isNullOrEmpty(indexName) ||Strings.isNullOrEmpty(document)){
            return "Parameters are wrong!";
        }
        EsDocument esDocument = new EsDocument();
        return esDocument.executeIndexRequest(indexName,document,esUtil);
    }
    @RequestMapping("/dg")
    public String executeGetRequest(String indexName,String document){
        if(Strings.isNullOrEmpty(indexName) ||Strings.isNullOrEmpty(document)){
            return "Parameters are wrong!";
        }
        EsDocument esDocument = new EsDocument();
        return esDocument.executeGetRequest(indexName,document,esUtil);
    }
    @RequestMapping("/dcde")
    public String executeCheckDocumentExistsRequest(String indexName,String document){
        if(Strings.isNullOrEmpty(indexName) ||Strings.isNullOrEmpty(document)){
            return "Parameters are wrong!";
        }
        EsDocument esDocument = new EsDocument();
        return esDocument.executeCheckExistIndexDocumentRequest(indexName,document,esUtil);
    }
    @RequestMapping("/ddid")
    public String executeDeleteIndexDocumentRequest(String indexName,String document){
        if(Strings.isNullOrEmpty(indexName) ||Strings.isNullOrEmpty(document)){
            return "Parameters are wrong!";
        }
        EsDocument esDocument = new EsDocument();
        return esDocument.executeDeleteIndexDocumentsRequest(indexName,document,esUtil);
    }
    @RequestMapping("/duid")
    public String executeUpdateIndexDocumentRequest(String indexName,String document){
        if(Strings.isNullOrEmpty(indexName) ||Strings.isNullOrEmpty(document)){
            return "Parameters are wrong!";
        }
        EsDocument esDocument = new EsDocument();
        return esDocument.executeUpdateIndexDocumentRequest(indexName,document,esUtil);
    }
    @RequestMapping("/dtv")
    public String executeTermVectorRequest(String indexName,String document,String field){
        if(Strings.isNullOrEmpty(indexName) ||Strings.isNullOrEmpty(document)){
            return "Parameters are wrong!";
        }
        EsDocument esDocument = new EsDocument();
        return esDocument.executeTermVectorRequest(indexName,document,field,esUtil);
    }

    @RequestMapping("/db")
    public String executeBulkReqeust(String indexName,String field){
        if(Strings.isNullOrEmpty(indexName) ||Strings.isNullOrEmpty(field)){
            return "Parameters are wrong!";
        }
        EsDocument esDocument = new EsDocument();
        return esDocument.executeBulkRequest(indexName,field,esUtil);
    }



}