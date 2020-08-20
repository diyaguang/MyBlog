package com.dygstudio.myblog.service.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.*;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

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
    private static Log log = LogFactory.getLog(EsUtil.class);
    private RestClient restClient;

    /*
     * 功能描述: 初始化 ESClient
     * 其中 @PostConstruct是在 java5引入的注解，在项目启动时会执行这个方法，或者说实在 Spring容器启动时执行。
     * 用来修饰一个非静态的 void() 方法（且只有一个），注解的方法不能有任何参数，返回值必须为 void，注解方法不得抛出已检查异常
     * 执行会在 构造函数之后，init() 方法之前。
     * 通常作为一些数据的常规化加载。修饰的方法会在服务器加载 Servlet的时候运行，并且只会被服务器执行一次，在构造函数之后执行。
     * 如果想在生成对象时候，完成某些初始化操作，偏偏这些初始化操作依赖于依赖注入，就无法在构造函数中实现，使用这个注解
     * 完成初始化，这个方法会在依赖注入完成后被自动调用。
     */
    @PostConstruct
    public void initEs(){
       /*  创建普通的 客户端连接
                restClient = RestClient.builder(
                new HttpHost("120.53.7.166",9200,"http"),
                new HttpHost("111.229.51.186",9200,"http")
        ).build();
        */

        /* 配置构建 RestClient是选择性的设置配置参数，使用 RestClientBuilder 对象进行配置 */
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("120.53.7.166",9200,"http"),
                new HttpHost("111.229.51.186",9200,"http")
        );

        //配置请求头
        Header[] defaultHeaders = new Header[]{new BasicHeader("header","value")};
        builder.setDefaultHeaders(defaultHeaders);

        //配置失败监听器
        //构建配置监听器，这个类型的监听器在每次节点失败时都会收到通知，在启用嗅探时在内部使用
        builder.setFailureListener(new RestClient.FailureListener(){
            @Override
            public void onFailure(Node node){
                //
            }
        });

        /* 配置节点选择器
         * 节点选择器，客户端可以解决服务器端多节点选择以及节点均衡处理的问题
         * restclient提供了四种选择器，HasAttributeNodeSelector,PreferHasAttributeNodeSelector,ANY,SKIP_DEDICATED_MASTERS
         * 都实现了 select() 方法
         * ANY是默认的，SKIP_DEDICATED_MASTERS（过滤掉 master，data，ingest）
         */
        builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

        /* 设置超时时间 */
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback(){
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder){
                return requestConfigBuilder.setSocketTimeout(10000);
            }
        });

        restClient = builder.build();
        log.info("ElasticSearch init in service.");
    }

    /*
     * 创建基础请求
     */
    public Request buildRequest(){
        Request request = new Request("GET","/");
        return request;
    }

    /*
     * 同步执行简单请求
     */
    public String executeRequest(){
        Request request = buildRequest();
        try {
            Response response = restClient.performRequest(request);
            return response.toString();
        }catch (Exception e){
            e.printStackTrace();
        }
        try {
            restClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return "Get result failed!";
    }

    /*
     * 异步执行简单请求
     * 配置 ResponseListener 监听，成功和失败有不同的处理函数
     */
    public String executeRequestAsync(){
        Request request = buildRequest();
        restClient.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                System.out.println("base request success");
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("base request failure "+e.getMessage());
            }
        });

        try {
            restClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return "Get result failed!";
    }

    public void closeEs(){
        try {
            restClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
