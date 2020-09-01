package com.dygstudio.myblog.service.common;

import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/* 搜索实战
* 允许用户执行搜索查询并返回匹配查询的搜索命中结果，可以跨一个或多个索引，以及跨一个或多个类型来执行
*  */
public class EsSearch {

    /* 搜索 API
    * 使用 SearchAPI，执行一个搜索查询，并返回与查询匹配的搜索点击，用户可以使用简单的查询字符串作为参数或使用请求主体提供查询
    * 基本搜索API是 "空搜索" ，不指定任何查询条件，只返回集群索引中的所有文档，实际使用过程中，一般不会用到空搜索
    * 需要构建 搜索请求 SearchRequest，可用于与搜索文档、聚合、Suggest(建议，提示) 有关的任何操作，而且还提供了请求高亮显示结果的方法
    * 创建该对象，需要依赖 SearchSourceBuilder，需要先构建 Query，然后添加到 Request中
    *
    * 测试测查询语句：http://localhost:8080/api/es/sr?field=message&value=age
    *  */

    public SearchRequest buildSearchRequest(String field,String value){
        SearchRequest searchRequest = new SearchRequest();
        //大多数搜索参数都添加到 SearchSourceBuilder 中，为进入搜索请求主体的所有内容提供 setter
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //searchSourceBuilder.query(QueryBuilders.matchAllQuery());

        //请求对象的可选配置

        //设置路由参数
        searchRequest.routing("routing");
        //设置 IndicesOption 控制方法
        searchRequest.indicesOptions(IndicesOptions.lenientExpand());
        //使用 首选参数，例如 执行搜索以首选本地分片，默认是在分片之间搜索的
        searchRequest.preference("_local");

        //在 SearchSourceBuilder 相关的可选配置
        //设置指定词向量的查询条件
        //说明：这里使用
        // matchQuery 表示模糊查询(还有个问题，就是查询的话，会是同 词向量 模糊查询)，例如 xxx Message xxxx ，这时候查询 Message会查询到，查询 age 则查询不到数据
        // termQuery 表示匹配词向量匹配查询，必须全部匹配
        //searchSourceBuilder.query(QueryBuilders.matchQuery(field,value));
        //设置搜索结果索引的起始地址，默认为0
        searchSourceBuilder.from(0);
        //设置要返回的搜索命中数的大小，默认为 10
        searchSourceBuilder.size(5);
        //设置一个可选的超时时间，控制允许搜索的时间
        searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));


        /* 生成查询
        * 搜索查询时基于 QueryBuilder 创建的，ES中每个搜索查询都需要用到 QueryBuilder，是基于 Elasticsearch DSL 实现的
        * QueryBuilder 既可以使用其构建函数来创建，也可以使用 QueryBuilders 工具来创建，工具提供了流式编程格式来创建 QueryBuilder
        *  */
        //方法 1
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder(field,value);
        //创建 QueryBuilder，提供配合搜索查询选项的方法（fuzziness：模糊性）
        // AUTO 表示模糊查询(还有个问题，就是查询的话，会是同 词向量 模糊查询)，例如 xxx Message xxxx ，这时候查询 Message会查询到，查询 age 则查询不到数据
        // FIELD 查询 message 字段的 age 值，报错 Elasticsearch exception [type=search_phase_execution_exception, reason=all shards failed]
        // ONE 结果为 表示模糊查询(还有个问题，就是查询的话，会是同 词向量 模糊查询)，例如 xxx Message xxxx ，这时候查询 Message会查询到，查询 age 则查询不到数据
        // TWO 结果为 表示模糊查询(还有个问题，就是查询的话，会是同 词向量 模糊查询)，例如 xxx Message xxxx ，这时候查询 Message会查询到，查询 age 则查询不到数据
        // ONE 结果为 表示模糊查询(还有个问题，就是查询的话，会是同 词向量 模糊查询)，例如 xxx Message xxxx ，这时候查询 Message会查询到，查询 age 则查询不到数据
        matchQueryBuilder.fuzziness(Fuzziness.AUTO);
        //在匹配查询上设置前缀长度
        matchQueryBuilder.prefixLength(3);
        //设置最大扩展选项以控制查询的模糊过程
        matchQueryBuilder.maxExpansions(10);

        //matchQueryBuilder = QueryBuilders.matchQuery("content","货币").fuzziness(Fuzziness.AUTO).prefixLength(3).maxExpansions(10);

        //添加 matchQueryBuilder 到 searchSourceBuilder 中
        searchSourceBuilder.query(matchQueryBuilder);

        /* 设定排序策略
        * ES 支持用户配置搜索结果的排序策略，常见的排序策略有按时间和按相关性排序两种
        * SearchSourceBuilder 允许添加一个或多个 SortBuilder 实例，有4种特殊的实现类，FieldSortBuilder，SourceSortBuilder，GeoDistanceSortBuilder，ScriptSortBuilder
        *  */
        //按分数降序排序（默认）
        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        //按 ID 升序排序
        //searchSourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));

        /* 筛选源
        * 默认情况下，搜索请求一般会返回文档源的内容，用户可以覆盖此行为，可以完全关闭源检索，在 SearchSourceBuilder 配置源搜索开关
        * fetch（获取）
        *  */
        searchSourceBuilder.fetchSource(false);
        //还可以设置要获取的 类 和不需要获取的列
        searchSourceBuilder.fetchSource(new String[]{"title","message"},new String[]{});
        //该方法还接收一个或多个通配符模式的数组，以便更细度方式控制那些字段被包括或被排除
        //String[] includeFields = new String[]{"title","innerObject"};
        //String[] excludeFields = new String[]{"user"};
        //searchSourceBuilder.fetchSource(includeFields,excludeFields);

        /* 请求高亮显示
        * 在 SearchSourceBuilder 上设置 HighlightBuilder，添加一个或多个 HighlightBuilder.Field 实例，可以为每个字段定义不同的高亮显示行为
        * */
        //HighlightBuilder highlightBuilder = new HighlightBuilder();
        //为 title 字段创建高亮字段
        //HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("title");
        //设置字段高亮类型
        //highlightBuilder.highlighterType("unified");
        //highlightBuilder.field(highlightTitle);
        //添加第二个高亮显示字段
        //HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        //highlightBuilder.field(highlightUser);
        //searchSourceBuilder.highlighter(highlightBuilder);

        /* 请求聚合
        * 可以设置请求聚合结果，需要先创建聚合构建器 AggregationBuilder，然后将其添加到 SearchSourceBuilder 中
        *  */
        //TermsAggregationBuilder aggregationBuilder = AggregationBuilders.terms("by_company").field("company.keyword");
        //aggregationBuilder.subAggregation(AggregationBuilders.avg("average_age").field("age"));
        //searchSourceBuilder.aggregation(aggregationBuilder);

        /* 建议请求
        * 设置请求结果，给出 自动提示（自动感知）
        * 在 ES中，要想在搜索请求中添加请求，需要使用 SuggestBuilder 工厂类，是SuggestionBuilder 类的实现类之一
        * SuggestionBuilder工厂类 需要添加到 顶级 SuggestBuilder 中，并将顶级 SuggestBuilder 添加到 SearchSourceBuilder中
        *  */
        //SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("content").text("货币");
        //SuggestBuilder suggestBuilder = new SuggestBuilder();
        //suggestBuilder.addSuggestion("suggest_user",termSuggestionBuilder);
        //searchSourceBuilder.suggest(suggestBuilder);

        /* 分析聚合查询
        * 从 ES 2.2 开始，提供 ProfileAPI，供用户 检索、聚合、过滤 执行时间和其他细节信息，帮助分析每次检索各个环节所用的时间
        * 使用时，用户必须在 SearchSourceBuilder 实例中将配置标志设置为 true
        *  */
       // searchSourceBuilder.profile(true);

        //将 searchSourceBuilder 添加到 SearchRequest 中
        searchRequest.source(searchSourceBuilder);
        return searchRequest;
    }

    public String executeSearchRequest(String field,String value,EsUtil esUtil){
        esUtil.initHEs();
        SearchRequest request = buildSearchRequest(field,value);

        try {
            SearchResponse response = esUtil.restHighLevelClient.search(request, RequestOptions.DEFAULT);
            return precessSearchResponse(response)+"; source string is:"+response.toString();
        }catch (Exception e){
            e.printStackTrace();
            return "execute SearchRequest error :"+e.getMessage();
        }
        finally {
            esUtil.closeHEs();
        }
    }
    private String precessSearchResponse(SearchResponse response){
        String resultText = "";
        if(response == null)
            return "the search response is null !";
        //获取 HTTP 状态码
        RestStatus status = response.status();
        //获取请求执行时间
        TimeValue took = response.getTook();
        //获取请求是否提前终止
        Boolean terminatedEarly = response.isTerminatedEarly();
        //获取请求是否超时
        boolean timeOut = response.isTimedOut();
        resultText+= "status is "+status+"; took is "+took+"; terminatedEarly is "+terminatedEarly+"; timedOut is "+timeOut;

        //搜索请求相关的分片
        //response 提供了有关搜索影响的分片奏疏，以及成功与失败的统计信息，并提供了有关分片级别执行的信息，失败的信息通过 ShardSearchFailure 实例数组元素处理
        //搜索成功的分片的统计信息
        int totalShards = response.getTotalShards();
        //搜索失败的分片的统计信息
        int successfulShards = response.getSuccessfulShards();
        int failedShards = response.getFailedShards();
        resultText+="; totalShards is "+totalShards+"; successfulShards is "+successfulShards+"; failedShards is "+failedShards;
        for (ShardSearchFailure failure : response.getShardFailures()){
            resultText+="; fail is "+failure.toString();
        }

        //获取搜索结果 （Search Hits）
        //其中包含了有关所有搜索结果的全部信息，如 点击总数，最高分数等
        SearchHits hits = response.getHits();
        //搜索结果的总数量
        TotalHits totalHits = hits.getTotalHits();
        long numHits = totalHits.value;
        //搜索结果的相关性数据
        TotalHits.Relation relation = totalHits.relation;
        float maxScore = hits.getMaxScore();
        resultText+="; numHits is "+numHits+"; maxScore is "+maxScore;

        //对 SearchHits 的解析还可以通过 遍历 SearchHit 数组来访问，提供了对文档的基本信息的访问，如索引名称，文档ID，每次搜索得分 等
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits){
            String index = hit.getIndex();
            String id = hit.getId();
            float score = hit.getScore();
            resultText+="; docId is "+id+"; docIndex is "+index+"; docScore is "+score;
            //ES 允许以简单的 JSON字符串或键值对形式返回文档源，
            // 映射中，常规字段由字段名作为键值，包含字段值，而多值字段作为对象列表返回，嵌套对象作为另一个键值返回
            String sourceAsString = hit.getSourceAsString();
            Map<String,Object> sourceAsMap = hit.getSourceAsMap();
            String documentTitle = (String)sourceAsMap.get("title");
            //List<Object> users = (List<Object>)sourceAsMap.get("user");
            //Map<String,Object> innerObject = (Map<String,Object>)sourceAsMap.get("innerObject");
            resultText+="; sourceAsString is "+sourceAsString+"; sourceAsMap size is "+sourceAsMap.size();

            //搜索结果高亮显示
            //Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            //HighlightField highlight = highlightFields.get("content");
            //Text[] fragments = highlight.fragments();
            //String fragmentString = fragments[0].string();
            //resultText+="; fragmentString is "+fragmentString;
        }

        //搜索聚合结果
        //通过 response 实例获取搜索的聚合结果，首先获取聚合树的根，即聚合对象，然后按照名称获取搜索聚合结果
        //Aggregations aggregations = response.getAggregations();
        //按 content 聚合
        //Terms byCompanyAggregation = aggregations.get("by_content");
        //获取 Elastic 为关键字的 buckets
        //Terms.Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic");
        //获取平均年龄的子聚合
        //Avg averageAge = elasticBucket.getAggregations().get("average_age");
        //double avg = averageAge.getValue();
        //resultText += "; avg is "+avg;

        //解析 Suggestions 结果
        //首先在 response 实例中使用 Suggest 对象作为入口点，然后检索嵌套的 Suggest对象
        //Suggest suggest = response.getSuggest();
        //按 content 搜索 Suggest
        //TermSuggestion termSuggestion = suggest.getSuggestion("content");
  /*      for(TermSuggestion.Entry entry : termSuggestion.getEntries()){
            for (TermSuggestion.Entry.Option option : entry){
                String suggestText = option.getText().string();
                resultText+="; suggestText is "+suggestText;
            }
        }*/

        return resultText;
    }



    /* 滚动搜索
    * 可通过搜索请求，获取大量的搜索结果，类似于数据库中的分页查询
    * 使用滚动搜索，对于大请求时，类似数据库中的游标，缓存数据集位置，用于后续的分页使用，ES会缓存查询结果一段时间，可设定用于非实时响应，用于处理大量数据，只反映 Search那一刻的数据
    *  */
}
