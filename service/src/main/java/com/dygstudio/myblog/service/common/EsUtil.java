package com.dygstudio.myblog.service.common;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.context.annotation.Configuration;

/**
 * 〈功能概述〉
 *
 * @className: EsUtil
 * @package: com.dygstudio.myblog.service.common
 * @author: diyaguang
 * @date: 2020/8/19 5:09 下午
 */
@Configuration
public class EsUtil {

    /*
     * 功能描述: 创建简单的 ES RestClient
     * @Param:
     * @Return:
     * @Author: diyaguang
     * @Date: 2020/8/19 5:33 下午
     */
    public RestClient buildEsRestClient(){
        RestClient restClient = RestClient.builder(
                new HttpHost("120.53.7.166",9200,"http"),
                new HttpHost("111.229.51.186",9200,"http")
        ).build();

        return restClient;
    }
}
