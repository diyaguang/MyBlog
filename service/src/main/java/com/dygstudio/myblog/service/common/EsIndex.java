package com.dygstudio.myblog.service.common;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.DetailAnalyzeResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 索引实战
 * 存储数据的行为就叫做索引，文档会属于一种类型，这些类型会存在于索引中。
 * 索引=》数据库，类型=》表，文档=》行数据，字段=》列数据
 * 索引作为名词时，好比关系数据库中的数据库，存储文档的地方，作为动词时，一个文档存储到索引中。
 * 索引还有倒排索引的意思，文档中的所有字段都会被素银，字段都有一个倒排索引，所有字段都可被索引
 */
public class EsIndex {

    /*
     * 字段索引分析
     * 当文档被索引到索引文件时，会进行分词操作，用户查询时，也会基于分词进行检索
     * 可以使用分析接口 AnalyzeAPI 来分析字段是如何建立索引的
     */

    /**
     * 构建分析请求
     *
     * @Param: []
     * @Return: void
     * @Author: diyaguang
     * @Date: 2020/8/24 2:09 下午
     */
    public AnalyzeRequest buildAnalyzerequest(String text) {
        AnalyzeRequest request = new AnalyzeRequest();
        //要包含的文本，多个字符串被视为多值字段
        request.text(text);
        //使用内置分析器
        request.analyzer("standard");
        //request.text("Some text to analyze","Some more text to analyze");
        //使用内置的英文分析器
        //request.analyzer("english");

        //自定义分析器1
        /*request.text("<b>Some text to analyze</b>");
        request.addCharFilter("html_strip");  //配置字符筛选器
        request.tokenizer("standard");  //配置标记器
        request.addTokenFilter("lowercase");  //添加内置标记筛选器*/

        //自定义分析器2
        /*Map<String,Object> stopFilter = new HashMap<>();
        stopFilter.put("type","stop");
        stopFilter.put("stopwords",new String[]{"to"}); //自定义令牌筛选器 tokenfilter 的配置
        request.addTokenFilter(stopFilter);  //添加自定义标记筛选器*/

        /* Request 对象的 可选的配置
        * 通过实验：如果 设置 request.explain(true) 则 response.getTokens() 将不会获得AnalyzeToken 数据
        * 并且不知道为啥，response.detail() 也获取不到  DetailAnalyzeResponse 信息  */
        /*request.explain(true);   //设置为 true 为响应添加更多详细信息
        request.attributes("keyword","type");   //设置属性，允许只返回用户感兴趣的令牌属性*/

        return request;
    }

    public void executeAnalyzeRequest(String text, EsUtil esUtil) {
        esUtil.initHEs();
        AnalyzeRequest request = buildAnalyzerequest(text);
        try {
            AnalyzeResponse response = esUtil.restHighLevelClient.indices().analyze(request, RequestOptions.DEFAULT);
            processAnalyzeResponse(response);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            esUtil.closeHEs();
        }
    }

    private void processAnalyzeResponse(AnalyzeResponse response) {
        //AnalyzeToken 报错了有关分析生成的单个令牌信息
        List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();
        if (tokens == null)
            return;
        for (AnalyzeResponse.AnalyzeToken token : tokens) {
            EsUtil.log.info(token.getTerm() + " start offset is " + token.getStartOffset() + ";end offset is " + token.getEndOffset() + ";position is " + token.getPosition());
        }
        //如果把 explain 设置为 true，通过 detail 方法返回信息。
        //DetailAnalyzeResponse 包含有关分析链中不同子步骤生成的令牌的更详细的信息
        DetailAnalyzeResponse detail = response.detail();
        if (detail == null)
            return;
        EsUtil.log.info("detail is " + detail.toString());
    }


    /* 创建索引 */

    /**
     * 功能描述: 创建索引请求对象
     * @Param: [index, shardsNumber, replicasNumber]
     * @Return: void
     * @Author: diyaguang
     * @Date: 2020/8/24 3:40 下午
     */
    public CreateIndexRequest buildIndexRequest(String index,int shardsNumber,int replicasNumber){
        CreateIndexRequest request = new CreateIndexRequest(index);
        //配置分片数量和副本数量
        request.settings(Settings.builder().put("index.number_of_shards",shardsNumber).put("index.number_of_replicas",replicasNumber));

        /*//还可以使用 字符串方式提供映射源
        request.mapping("", XContentType.JSON);

        //以 Map 方式提供映射
        Map<String,Object> message = new HashMap<>();  //创建属性项
        message.put("type","text");
        Map<String,Object> properties = new HashMap<>();   //创建属性集合
        properties.put("message",message);
        Map<String,Object> mapping = new HashMap<>();  //创建 Mapping
        mapping.put("properties",properties);
        request.mapping(mapping);

        //以 XContentBuilder 方式提供映射源
        try{
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.startObject("properties");
                {
                    builder.startObject("message");
                    {
                        builder.field("type","text");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping(builder);
        }catch (Exception e){
            e.printStackTrace();
        }finally {

        }*/

        /* 创建索引过程中，还可以为索引设置别名，有两种方式
        * 一种是在创建索引时设置
        * 一种是通过提供整个索引源来设置 */
        //创建索引时设置
        //request.alias(new Alias(index+"_alias").filter(QueryBuilders.termQuery("user","diyaguang")));
        //通过提供整个索引源
        //request.source("",XContentType.JSON);  //这个其中设置了 整个索引的信息，包括 setting,mappings,aliases 的相关设置

        /* 可选配置参数 */
        //等待所有节点确认创建索引的超时时间
        request.setTimeout(TimeValue.timeValueMinutes(2));
        //从节点连接到主节点的超时时间
        request.setMasterTimeout(TimeValue.timeValueMinutes(2));
        //在请求响应返回前活动状态的分片数量
        request.waitForActiveShards(ActiveShardCount.from(2));
        //在请求影响返回前所动状态的拷贝数量
        request.waitForActiveShards(ActiveShardCount.DEFAULT);

        return request;
    }

    public void executeCreateIndexRequest(String index,EsUtil esUtil){
        CreateIndexRequest request = buildIndexRequest(index,3,2);
        esUtil.initHEs();
        try{
            CreateIndexResponse createIndexResponse = esUtil.restHighLevelClient.indices().create(request,RequestOptions.DEFAULT);
            processCreateIndexResponse(createIndexResponse);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processCreateIndexResponse(CreateIndexResponse createIndexResponse){
        boolean acknowledged = createIndexResponse.isAcknowledged();
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        EsUtil.log.info("acknowledged is "+acknowledged+" ;shardsAcknowledged is "+shardsAcknowledged);
    }


    /* 获取索引 */

    public GetIndexRequest buildGetIndexRequest(String index){
        GetIndexRequest request = new GetIndexRequest(index);
        //设置为 true，对于未在索引上显示设置的内容，将返回默认值
        request.includeDefaults(true);
        //控制解析不可用与索引及展开通配符表达式
        request.indicesOptions(IndicesOptions.lenientExpand());
        return request;
    }

    public void executeGetIndexRequest(String index){

    }

}
