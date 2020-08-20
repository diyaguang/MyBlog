package com.dygstudio.myblog.service.controller;

import com.dygstudio.myblog.service.common.EsUtil;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 〈功能概述〉
 *
 * @className: EsTestController
 * @package: com.dygstudio.myblog.service.controller
 * @author: diyaguang
 * @date: 2020/8/20 10:11
 */
@RestController
@RequestMapping("/api/testes")
public class EsTestController {

    @Resource
    private EsUtil esUtil;

    @RequestMapping("/init")
    public String initElasticSearch(){
        esUtil.initEs();
        return "Init ElasticSearch Over!";
    }

    @RequestMapping("/baseRequest")
    public String executeRequest(){
        String result = esUtil.executeRequest();
        return result;
    }
    @RequestMapping("/baseAsyncRequest")
    public String executeAsyncRequest(){
        String result = esUtil.executeRequestAsync();
        return result;
    }
}
