package com.dygstudio.myblog.service.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.*;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.*;
import org.elasticsearch.client.sniff.ElasticsearchNodesSniffer;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

/**
 * 〈功能概述〉
 *
 * @className: EsUtil
 * @package: com.dygstudio.myblog.service.common
 * @author: diyaguang
 * @date: 2020/8/19 5:09 下午
 */
public class EsUtil {
    private static Log log = LogFactory.getLog(EsUtil.class);

    /*
     * 高级客户端是基于初级客户端来实现的，主要目标是公开特定的 API方法，接收请求作为参数并返回响应结果
     * 有同步 和 异步 两种调用方式，异步需要配置监听器才能使用。
     * 高级客户端 需要 java1.8 以上，依赖于 Elasticsearch core 项目，版本要与ES版本同步
     * 高级客户端能与运行着相同主版本和更高版本上的任何 ES 节点进行有效通信
     * 需要在 Maven 中引用高级客户端的依赖
     * org.elasticsearch.client | elasticsearch-rest-high-level-client | 7.2.1
     * org.elasticsearch.client | elasticsearch-rest-client | 7.2.0
     * org.elasticsearch | elasticsearch | 7.2.1
     * 高级客户端的初始化 是基于 RestHighLevelClient 实现的，RestHighLevelClient 是基于初级客户端生成构建的
     * 原理：高级客户端在内部创建执行请求的初级客户端，初级客户端会维护一个连接池，并启动一些线程，当高级客户端
     * 的接口调用完成时，应该关闭它，因为它将同步关闭内部初级客户端，以释放这些资源。
     */
    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;
    private Sniffer sniffer;

    //设置全局单实例 RequestOption，创建好后尅在发出请求时使用
    private static final RequestOptions COMMON_OPTIONS;

    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        //AddHeader用于授权或在ES前使用代理所需的头信息
        builder.addHeader("Authorization", "diyaguang " + "my-token");
        /* setHttpAsyncResponseConsumerFactory 自定义响应消费者 提供谓词，终节点，可选查询字符串参数，可选请求主体
         * 以及用于为每个请求尝试创建org.apache.http.nio.protocol.HttpAsyncResponseConsumer回调实例的可选工厂 来发送异步请求。
         * 控制响应正文如何从客户端的非阻塞HTTP连接进行流式传输。
         * 如果未提供，则使用默认实现，将整个响应主体缓存在堆内存中，最大为100 MB。
         * HeapBufferedResponseConsumerFactory 属于 Java REST客户端
         */
        builder.setHttpAsyncResponseConsumerFactory(
                new HttpAsyncResponseConsumerFactory
                        .HeapBufferedResponseConsumerFactory(30 * 1024 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    /*
     * 初始化高级别客户端
     * 在 高级别客户端中，请求对象 Request，请求结果的解析，与初级客户端的用法相同。
     * 与之类似的 RequestOptions 的使用以及客户端的常见设置相同
     */
    public void initHEs(){

        restHighLevelClient = new RestHighLevelClient(RestClient.builder(
                new HttpHost("120.53.7.166", 9200, "http"),
                new HttpHost("111.229.51.186", 9200, "http")
        ));

        log.info("ElaseticSearch init in service.");
    }
    /*
     * 功能描述: 初始化 ESClient
     * 其中 @PostConstruct是在 java5引入的注解，在项目启动时会执行这个方法，或者说实在 Spring容器启动时执行。
     * 用来修饰一个非静态的 void() 方法（且只有一个），注解的方法不能有任何参数，返回值必须为 void，注解方法不得抛出已检查异常
     * 执行会在 构造函数之后，init() 方法之前。
     * 通常作为一些数据的常规化加载。修饰的方法会在服务器加载 Servlet的时候运行，并且只会被服务器执行一次，在构造函数之后执行。
     * 如果想在生成对象时候，完成某些初始化操作，偏偏这些初始化操作依赖于依赖注入，就无法在构造函数中实现，使用这个注解
     * 完成初始化，这个方法会在依赖注入完成后被自动调用。
     */
    //@PostConstruct
    public void initEs() {
       /*  创建普通的 客户端连接
                restClient = RestClient.builder(
                new HttpHost("120.53.7.166",9200,"http"),
                new HttpHost("111.229.51.186",9200,"http")
        ).build();
        */

        /* 配置构建 RestClient是选择性的设置配置参数，使用 RestClientBuilder 对象进行配置 */
        RestClientBuilder builder = RestClient.builder(
                new HttpHost("120.53.7.166", 9200, "http"),
                new HttpHost("111.229.51.186", 9200, "http")
        );

        //配置请求头
        Header[] defaultHeaders = new Header[]{new BasicHeader("header", "value")};
        builder.setDefaultHeaders(defaultHeaders);

        //配置失败监听器
        //构建配置监听器，这个类型的监听器在每次节点失败时都会收到通知，在启用嗅探时在内部使用
        builder.setFailureListener(new RestClient.FailureListener() {
            @Override
            public void onFailure(Node node) {
                //
            }
        });

        /* 设置超时时间 */
        // RestClient 使用 requestconfigCallback 的实例来完成超时设置，
        // 该接口方法接收 requestconfig.builder 的实例作为参数，并且具有相同类型的返回类型。
        builder.setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
            @Override
            public RequestConfig.Builder customizeRequestConfig(RequestConfig.Builder requestConfigBuilder) {
                return requestConfigBuilder.setSocketTimeout(10000);//.setSocketTimeout(60000);
            }
        });

        /* 线程设置 */
        // Apache HTTP 异步客户端默认启用一个调度程序线程，连接管理器使用的多个工作线程。
        // 线程数主要取决于 Runtime.getRuntime().availableProcessors() 返回的结果。ES 允许用户修改线程数
        Integer number = 5;
        builder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultIOReactorConfig(
                        IOReactorConfig.custom().setIoThreadCount(number).build()
                );
            }
        });

        /* 配置节点选择器
         * 节点选择器，客户端可以解决服务器端多节点选择以及节点均衡处理的问题
         * restclient 提供了四种选择器，HasAttributeNodeSelector,PreferHasAttributeNodeSelector,ANY,SKIP_DEDICATED_MASTERS
         * 都实现了 select() 方法
         * ANY是默认的，SKIP_DEDICATED_MASTERS（过滤掉 master，data，ingest）
         */
        builder.setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

        /* 节点选择器设置 (自定义的方式）
        * 默认情况下，客户端会轮训方式将每个请求方式送到配置的各个节点
        * ES允许自由选择需要连接的节点，可以通过初始化客户端来配置节点选择器
        * 配置后 对于每个请求，客户端都通过节点选择器来筛选备选节点。
        */
        /*builder.setNodeSelector(new NodeSelector() {
            @Override
            public void select(Iterable<Node> nodes) {
                boolean foundOne = false;
                for(Node node : nodes){
                    String rackId = node.getAttributes().get("rack_id").get(0);
                    if("targetId".equals(rackId)){
                        foundOne = true;
                        break;
                    }
                }
                if (foundOne){
                    Iterator<Node> nodesIt = nodes.iterator();
                    while(nodesIt.hasNext()){
                        Node node = nodesIt.next();
                        String rackId = node.getAttributes().get("rack_id").get(0);
                        if("targetId".equals(rackId)==false){
                            nodesIt.remove();
                        }
                    }
                }
            }
        });*/

        /* 配置嗅探器
         * 在客户端启动时配置嗅探器外，还可以在失败时启动嗅探器，意味着每次失败后，节点列表都会立即更新，而不是在接下来的普通嗅探器循环中更新
         * 需要先创建一个 SniffOnFailureListener，然后在创建 RestClient 时配置，
         * 在创建后，同一个 SniffOnFailureListener实例会相互关联，以便在每次失败时候都通知该实例，并使用 嗅探器执行嗅探动作
         */
        SniffOnFailureListener sniffOnFailureListener = new SniffOnFailureListener();
        builder.setFailureListener(sniffOnFailureListener);

        /* Build 出 RestClient */
        restClient = builder.build();

        /* 配置嗅探器
         * 允许自动发现运行中的 ES 集群中的节点，并将其设置为现有的 RestClient 实例
         * 默认情况下，使用 nodes info  API 检索属于集群的节点，并使用 jackson 解析获得的 JSON 响应
         * 需要设置 Maven依赖：org.elasticsearch.client | elasticsearch-rest-client-sniffer | 7.2.1
         * 创建好 RestClient 实例，就可以将嗅探器与其进行关联了，嗅探器利用 RestClient提供的定期机制（默认5分钟）
         * 从集群中获取当前节点列表，并通过 RestClient类中的 setNodes 方法来更新它们
         * 通过 setSniffIntervalMillis 以毫秒为单位，之定义此间隔
         * -------------------------------------------------------------------------------------
         * ES 节点信息 API 不会返回连接到节点时要使用的协议，而是只返回他们的 host：port，默认会使用 HTTP
         * 如果需要使用 HTTPS，则必须手动创建并提供 ElasticSearchNodesNiffer 实例
         */
         sniffer = Sniffer.builder(restClient).setSniffIntervalMillis(60000).build();

         /* 配置嗅探器
          * ES 节点信息 API 不会返回连接到节点时要使用的协议，而是只返回他们的 host：port，默认会使用 HTTP
          * 如果需要使用 HTTPS，则必须手动创建并提供 ElasticSearchNodesNiffer 实例
          */
        /*NodesSniffer nodesSniffer = new ElasticsearchNodesSniffer(restClient,
                ElasticsearchNodesSniffer.DEFAULT_SNIFF_REQUEST_TIMEOUT,
                ElasticsearchNodesSniffer.Scheme.HTTPS);
        sniffer = Sniffer.builder(restClient).setNodesSniffer(nodesSniffer).build();*/




        log.info("ElasticSearch init in service.");
    }

    /*
     * 创建基础请求
     */
    public Request buildRequest() {
        Request request = new Request("GET", "/");
        // 可以在请求中添加参数
        request.addParameter("pretty", "true");
        // 可以将请求主题设置为 任意 HttpEnity，
        request.setEntity(new NStringEntity("{\"json\":\"text\"}", ContentType.APPLICATION_JSON));
        //还可以设置为一个字符串，在ES中，默认使用 application/json 的内容格式
        request.setJsonEntity("{\"json\":\"text\"}");
        // Request还有一些可选的请求构建选项，通过 RequestOptions 来实现
        // 在 RequestOption类中保存的请求，
        // 可以在同一个应用程序的多个请求之间共享，可以创建单一实例，在所有请求之间共享。
        //request.setOptions(COMMON_OPTIONS);

        // ES允许用户根据每个请求可以定制这些选项，例如：
        RequestOptions.Builder options = COMMON_OPTIONS.toBuilder();
        options.addHeader("title", "any other things");
        //request.setOptions(COMMON_OPTIONS);

        return request;
    }

    /*
     * 同步执行简单请求
     */
    public String executeRequest() {
        Request request = buildRequest();
        try {
            Response response = restClient.performRequest(request);
            /*
             * 已执行请求的信息
             * -----------------------------------------------------------------------------------
             * RequestLine 请求行，HTTP请求的组成部分，包含格式：Method SP Request-URI SP HTTP-Version CRLF
             * 其中Request-URI是客户端使用URLEncode之后的URI，即如果元素URI中包含非ASCII字符，客户端必须将其编码为 %编码值 的形式。
             * 分为4段：第一段是HTTP方法名，即GET、POST、PUT等。第二段是请求路径，如 /index.html第三段是HTTP协议版本，一般是HTTP/1.1。最后是一个CRLF回车换行。
             * 除了CRLF，段与段之间用空格分隔。
             * 综上所述，解析步骤为：去掉行尾的CRLF，并trim两端的空格。使用空格将字符串分为大小为3的数组a。method是a[0]，Request-URI是a[1]，HTTP-Version是a[2]。
             */
            RequestLine requestLine = response.getRequestLine();
            //返回 host 信息
            HttpHost host = response.getHost();
            //获取 响应状态行，从中解析状态代码
            int statusCode  = response.getStatusLine().getStatusCode();
            //获取响应头，通过 getheader(string) 按名称获取
            Header[] headers = response.getHeaders();
            //获取响应体内容
            String responseBody = EntityUtils.toString(response.getEntity());
            log.info("parse ElasticSearch Response , responseBody is :"+responseBody);
            return response.toString()+" ||| "+responseBody;
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            restClient.close();
            if(sniffer!=null)
                sniffer.close();   //关闭嗅探器
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Get result failed!";
    }

    /*
     * 异步执行简单请求
     * 配置 ResponseListener 监听，成功和失败有不同的处理函数
     */
    public String executeRequestAsync() {
        Request request = buildRequest();
        restClient.performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                System.out.println("base request success");
            }

            @Override
            public void onFailure(Exception e) {
                System.out.println("base request failure " + e.getMessage());
            }
        });

        try {
            restClient.close();
            if(sniffer!=null)
                sniffer.close();   //关闭嗅探器
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "Get result failed!";
    }

    /*
     * 多个并行异步操作
     * HttpEntity 类型：网络请求类型，既可以代表二级制内容，又可以代表字符内容，支持字符编码
     * 实体实在使用封闭内容执行请求，或当请求已经成功执行，或当响应体结果发送到客户端创建的
     * HttpEntity 和 @RequestBody或@ResponseBody 很像，能够访问请求和响应体，也能访问请求和响应头
     * HttpEntity 是一个接口，有很多类型的实现 BasicHeepEntity，BufferedHttpEntity，ByteArrayEntity，EntityTemplate 等等
     * -----------------------------------------------------------------------------------------
     * CountDownLatch 同步工具类，用来协调多个线程之间的同步。或者说是线程之间的通信。
     * 使一个线程等待其他线程各自执行完毕后再执行，通过一个计数器实现，通过 countDown()方法递减计数器
     * 任何调用这个对象上的 await() 方法的线程会被挂起，会等待直到 count值为0才会继续执行
     */
    public void multiExecuteRequest(HttpEntity[] documents) {
        final CountDownLatch latch = new CountDownLatch(documents.length);
        for (int i = 0; i < documents.length; i++) {
            Request request = new Request("PUT", "/posts/doc/" + i);
            request.setEntity(documents[i]);
            restClient.performRequestAsync(request, new ResponseListener() {
                @Override
                public void onSuccess(Response response) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                }
            });
        }
        try {
            latch.await();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void closeEs() {
        try {
            restClient.close();
            if(sniffer!=null)
                sniffer.close();   //关闭嗅探器
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    public void closeHEs(){
        try {
            restHighLevelClient.close();
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
