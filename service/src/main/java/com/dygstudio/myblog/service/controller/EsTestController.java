package com.dygstudio.myblog.service.controller;

import com.dygstudio.myblog.service.common.EsIndex;
import com.dygstudio.myblog.service.common.EsUtil;
import org.elasticsearch.common.Strings;
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
public class esTestController {


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
}