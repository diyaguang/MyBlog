package com.dygstudio.myblog.service.controller;

import com.dygstudio.myblog.service.common.EsUtil;
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
@RequestMapping("/api/testes")
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
}
