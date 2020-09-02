package com.dygstudio.myblog.service.common;

import com.fasterxml.jackson.datatype.jsr310.deser.InstantDeserializer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesIndexResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.rankeval.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
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

import java.util.*;
import java.util.concurrent.TimeUnit;

/* 搜索实战
* 允许用户执行搜索查询并返回匹配查询的搜索命中结果，可以跨一个或多个索引，以及跨一个或多个类型来执行
*  */
public class EsSearch {

    /* 搜索过程解析
    * 对已知文档的搜索：
    * 如果被搜索的文档能够从主分片或任意一个副本分片中被检索到，则与索引文档过程相同，对已知文档的搜索也会用到路由算法
    * shard = hash[routing] % number_of_primary_shards
    * 客户端发送文档的 Get请求，主节点使用路由算法算出文档所在的主分片，随后协同节点将请求转发个主分片所在的节点，也可以基于轮询算法转发给副分片
    * 请求节点一般会为每个请求选择不同的分片，一般采用轮询算法循环在所有副本分片中进行请求
    *
    * 对未知文档的搜索：
    * 大部分请求实际上是不知道查询条件会命中那些文档，搜索请求的执行不得不去询问每个索引中的每个分片
    * ES搜索过程 分为 查询阶段(Query Phase)，获取阶段(Fetch  Phase)
    * 在查询阶段，查询请求会广播到索引中的每个主分片和备份中，每个分片都会在本地执行检索，并在本地建立一个优先级队列（Priority Queue），该优先级队列是一份根据文档相关度质变进行排序的列表，长度由 from 和 size 两个分页参数决定
    * 查询阶段可以分为三个小的子阶段
    * 1.客户端你发送一个检索请求个某节点A，A会创建一个空的优先级队列，并跑【行知道分页参数 from 和size
    * 2.节点A 将搜索请求发送给该索引中的每一个分片，每个肥牛片在本地执行检索，并将结果添加到本地优先级队列中
    * 3.每个分片返回本地优先级序列中所记录的ID 与 sort 值，并发送给节点A，节点A将这些值合并到自己的本地优先级队列中，并作出全局的排序
    * 在获取阶段，主要是基于上一阶段找到所有搜索文档数据的具体文职，将文档数据内容取回并返回给客户端
    * ES中 默认的搜索类型就是 Query then Fetch，有可能会出现打分偏离的情形，ES 还提供了一个 DFS Query then Fetch 的搜索方式，和 Query then Fetch 基本你想通，会执行一个预查询来计算你整体文档的 frequency
    * 其过程：
    * 1.预查询每个分片，询问 Term 和 Document Frequency 等信息
    * 2.发送查询请求到每个分片
    * 3.找到各个分片中所有匹配的文档，并使用全局的 Trem/Document Frequency信息进行打分，在执行过程中依然要对结果构建一个优先队列
    * 4.返回结果的元数据到请求节点，此时世纪文档还没有发送到请求节点，发送的只是分数
    * 5.请求节点将所有分片的分数合并起来，并在请求节点上进行排序，文档被按照查询要求进行选择，最终，实际文档从他们各自所在的独立的分片上被检索出来，结果被返回给读者
    *
    * 对词条的搜索
    * ES 分别为每个文档中的字段建立了一个倒排索引，ES为了能快速找到某个词条，对所有的词条机型了排序，随后使用二分法查找词条，排序词条的集合也称为 Term Dictionary
    * 为了能提高查询性能，ES直接通过内存查找词条，非从磁盘中读取，但当词条太多时，放在内存也不太显示，引入了 Term  Index
    * Term Index 就像字典中的索引页，其中你的内容如字母 A开头的有哪些词条，这些词条分别在哪页，通过 Term Index，ES可以跨速定位到 Term Dictionary 的某个OffSet，然后从这个位置再往后顺序查找
    * 在实际应用中，更常见的往往是多个词条拼成的“联合查询”，核心思想是利用 跳表快速做“与”运算，还有一种方式是利用“BitSet”位图，按位“与”运算
    *  */

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
        //searchSourceBuilder.fetchSource(false);  //经过测试，设置了这个值后，将获取不到数据
        //还可以设置要获取的 类 和不需要获取的列
        //searchSourceBuilder.fetchSource(new String[]{"title","message"},new String[]{});   //经过测试，设置了这个配置后，只显示 title，message 两个字段内容了
        //该方法还接收一个或多个通配符模式的数组，以便更细度方式控制那些字段被包括或被排除
        //String[] includeFields = new String[]{"title","innerObject"};
        //String[] excludeFields = new String[]{"user"};
        //searchSourceBuilder.fetchSource(includeFields,excludeFields);

        /* 请求高亮显示
        * 在 SearchSourceBuilder 上设置 HighlightBuilder，添加一个或多个 HighlightBuilder.Field 实例，可以为每个字段定义不同的高亮显示行为
        * 经过测试：在 Source中会包含 "highlight":{"message":["这个是测试数据 用来测试 Message的。"]} 这种数据
        * */
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        //为 title 字段创建高亮字段
        HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("message");
        //设置字段高亮类型
        highlightBuilder.highlighterType("unified");
        highlightBuilder.field(highlightTitle);
        //添加第二个高亮显示字段
        //HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
        //highlightBuilder.field(highlightUser);
        //添加高亮配置
        searchSourceBuilder.highlighter(highlightBuilder);

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
        * 经过测试，删除文档中会增加 "suggest":{"suggest_message":[{"text":"测","offset":0,"length":1,"options":[]},{"text":"试","offset":1,"length":1,"options":[]}]} 输出
        *  */
        SuggestionBuilder termSuggestionBuilder = SuggestBuilders.termSuggestion("message").text("测试");
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("suggest_message",termSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);

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
            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField highlight = highlightFields.get("message");
            Text[] fragments = highlight.fragments();
            String fragmentString = fragments[0].string();
            resultText+="; fragmentString is "+fragmentString;
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
        Suggest suggest = response.getSuggest();
        //按 content 搜索 Suggest
        TermSuggestion termSuggestion = suggest.getSuggestion("suggest_message");
        for(TermSuggestion.Entry entry : termSuggestion.getEntries()){
            for (TermSuggestion.Entry.Option option : entry){
                String suggestText = option.getText().string();
                resultText+="; suggestText is "+suggestText;
            }
        }

        return resultText;
    }



    /* 滚动搜索
    * 可通过搜索请求，获取大量的搜索结果，类似于数据库中的分页查询
    * 使用滚动搜索，对于大请求时，类似数据库中的游标，缓存数据集位置，用于后续的分页使用，
    * ES会缓存查询结果一段时间，可设定用于非实时响应，用于处理大量数据，只反映 Search那一刻的数据
    * 构建滚动索引 API，ES会检测到滚动搜索参数的存在，并在相应的时间间隔内保持搜索上下文活动
    *  */
    private SearchRequest buildExecuteScrollSearchRequest(String indexName,int size){
        //设置索引
        //创建 SearchRequest 及详细的 SearchSourceBuilder，还可设置返回多少结果
        SearchRequest request = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title","is"));

        searchSourceBuilder.size(size);
        request.source(searchSourceBuilder);
        request.scroll(TimeValue.timeValueMinutes(1L));   //设置滚动间隔
        return request;
    }
    public String executeScrollSearchRequest(String indexName,String size,EsUtil esUtil){
        esUtil.initHEs();
        SearchRequest request = buildExecuteScrollSearchRequest(indexName,Integer.parseInt(size));
        try {
            SearchResponse response = esUtil.restHighLevelClient.search(request,RequestOptions.DEFAULT);
            //返回的滚动 ID，该ID指向保持活动状态的搜索上下文，并在后续的搜索滚动中需要使用
            //测试后返回 ScrollID：DnF1ZXJ5VGhlbkZldGNoAwAAAAAAAAEiFnNtcjRjRjJsVHV1eDRoWWw4ZWlaQUEAAAAAAAABIxZzbXI0Y0YybFR1dXg0aFlsOGVpWkFBAAAAAAAAAKEWTDBRRnNGb0lTY2VCMDQ0WHJ0aUJxQQ==
            String scrollId = response.getScrollId();
            //第一次滚动获取的结果
            SearchHits hits = response.getHits();
            return "ScrollId is: "+scrollId+"; hits is: "+hits.toString()+"; source is: "+response.toString();
        }catch (Exception e){
            e.printStackTrace();
            return "execute scroll search request is error :"+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    /* 滚动检索所有文档
    * 在 SearchScrollRequest 中设置上下文提及的滚动标识符和新的滚动间隔
    * 其次在设置好 SearchScrollRequest 后，将其传送给 searchScroll 方法
    * 请求发出后，ES服务器会返回另一批带有新额关东标识符的结果，依次类推，用户需要在新的 SearchScrollRequest中设置前文提及的滚动标识符和新的滚动间隔，以便获取下一次的结果
    * 这个过程重复执行，直到不再返回任何结果，意味着搜索完毕，所有匹配的文档都被检索了。
    * */

    public String executeAllScrollSearchRequest(String indexName,String size,EsUtil esUtil){
        String resultText = "";
        esUtil.initHEs();
        SearchRequest request = buildExecuteScrollSearchRequest(indexName,Integer.parseInt(size));
        try {
            SearchResponse searchResponse = esUtil.restHighLevelClient.search(request,RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId();
            SearchHits hits = searchResponse.getHits();
            resultText+=" scrollId is "+scrollId;
            resultText+="total hits is "+hits.getTotalHits().value+"; now hits is "+hits.getHits().length;

            while (hits != null && hits.getHits().length != 0){
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                //设置滚动搜索的过期时间，如果没有设置滚动标识符，则一但初始滚动时间过期，则滚动搜索的上下文也会过期
                scrollRequest.scroll(TimeValue.timeValueSeconds(30));
                SearchResponse searchScrollResponse = esUtil.restHighLevelClient.scroll(scrollRequest,RequestOptions.DEFAULT);
                scrollId = searchScrollResponse.getScrollId();
                hits = searchScrollResponse.getHits();
                resultText+=" scrollId is "+scrollId;
                resultText+="total hits is "+hits.getTotalHits().value+"; now hits is "+hits.getHits().length;
            }
            return resultText;
        }catch (Exception e){
            e.printStackTrace();
            return "execute all scroll search request is error :"+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }

    /* 清除滚动搜索的上下文
    * 使用 Clear Scroll API 删除最后一个滚动标识，以释放滚动搜索的上下文，当滚动搜索超时时间到期时，这个过程也会自动发生
    * 一般在滚动搜索会话后，需要立即执行清除滚动搜索的上下文
    * 使用时，需要构建清除滚动搜索的请求，ClearScrollRequest，需要把滚动标识符作为参数输入
    *  */
    private ClearScrollRequest buildClearScrollRequest(String scrollId){

        ClearScrollRequest request = new ClearScrollRequest();
        //添加单个滚动标识符
        request.addScrollId(scrollId);
        //还可以添加多个滚动标识符
        //List<String> scrollIds = new ArrayList<>();
        //scrollIds.add(scrollId);
        //request.setScrollIds(scrollIds);

        return request;
    }
    public String executeClearScrollRequest(String scrollId,EsUtil esUtil){
        esUtil.initHEs();
        ClearScrollRequest request = buildClearScrollRequest(scrollId);
        try {
            ClearScrollResponse response = esUtil.restHighLevelClient.clearScroll(request,RequestOptions.DEFAULT);
            //如果请求成功，则会返回 true
            boolean success = response.isSucceeded();
            //返回已释放的搜索上下文数
            int released = response.getNumFreed();
            return "success is "+success+"; released is "+released;
        }catch (Exception e){
            e.printStackTrace();
            return "execute clear scroll search request is error :"+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }


    /* 批量搜索
    * 批量搜索 API ，MultiSearch API，构建 MultiSearchRequest 对象，初始化时，搜索请求为空，需要把要执行的所有所有添加到 MultiSearchRequest中
    * 使用  request.add( xxxxxx ) 的方法来添加 SearchRequest请求，SearchRequest请求的创建方式同之前
    *
    * 解析 MultiSearchResponse 响应结果
    * response结果中包含 MultiSearchResponse.item 对象列表，每个对象对应每个搜索请求结果的响应
    * 使用 MultiSearchResponse.item 对象的 getFailure 方法中都会包含一个异常信息；反之如果请求执行成功，
    * 每个 MultiSearchResponse.item 对象的 getResponse 方法中获得 SearchResponse 并解析对应的结果信息
    *
    * Item[] items = response.getResponse();
    * for(Item item:items){
    *   Exception exception = item.getFailure();
    *   if(exception!=null) .....
    *   SearchResponse searchResponse = item.getResponse();
    *   SearchHits hits = searchResponse.getHits();
    *   if(hits.getTotalHits().value() <= 0){
    *       return;
    *   }
    *   SearchHit[] hitArray = hits.getHits();
    *   return "id is "+hitArray[0].getId()+"; index is "+hitArray[0].getIndex()+"; source is "+hitArray[0].getSourceAsString();
    * }
    * */



    /* 跨索引字段搜索
    * 跨索引的字段搜索接口 Field  Capabilities  API，需要先创建 FieldCapabilitiesRequest 请求，包含了要搜索的字段列表以及一个可选的目标索引名称列表
    * 如果没有提供目标索引名称列表，默认对所有索引执行相关的请求，字段列表支持通配符的表示方式，如 text_* 将返回与表达式匹配的所有字段
    * FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().fields("content").indices("index1","index2");
    * 有个可选的配置 request.indicesOptions(IndicesOptions.lenientExpandOpen());
    *
    * 执行请求：
    * FieldCapabilitiesResponse 中包含了每个索引中数据能否被搜索和聚合的信息，还包含了被搜索字段在对应索引中的贡献值
    * FieldCapabilitiesResponse response = restClient.fieldCaps(reqeust,RequestOptions.DEFAULT);
    *
    * 解析 FieldCapabilitiesResponse 响应
    *  */
    public void processFieldCapabilitiesResponse(FieldCapabilitiesResponse response,String field,String[] indices){
        //获取字段中可能含有的类型的映射
        Map<String, FieldCapabilities> fieldResponse = response.getField(field);
        Set<String> set = fieldResponse.keySet();
        //获取文本字段类型下的数据
        FieldCapabilities textCapabilities = fieldResponse.get("text");
        //数据能否被搜索到
        boolean isSearchable = textCapabilities.isSearchable();
        // is Aggregatable is isSearchable
        //数据能否聚合
        boolean isAggregatable = textCapabilities.isAggregatable();
        // is Aggregatable is isAggregatable
        //获取特定字段类型下的索引
        String[] indicesArray = textCapabilities.indices();
        if(indicesArray!=null){
            // "indicesArray is "+indicesArray.length
        }
        //field字段不能被搜索到的索引集合
        String[] nonSearchableIndices = textCapabilities.nonSearchableIndices();
        if(nonSearchableIndices!=null){
            // "nonSearchableIndices is "+nonSearchableIndices.length
        }
        //field字段不能被聚合到的索引集合
        String[] nonAggregatableIndices = textCapabilities.nonAggregatableIndices();
        if(nonAggregatableIndices!=null){
            // "nonAggregatableIndices is "+nonAggregatableIndices.length
        }
    }



    /* 搜索结果的排序评估
    * ES 提供了对搜索结果进行排序评估的接口，Ranking Evaluation API，ES提供了 rankeval 方法，对一组搜索请求的结果进行排序评估，以便衡量搜索结果的质量
    * 首先为搜索请求提供一组手动评级的文档，随后评估批量搜索请求的质量，并计算搜索相关指标，如 返回结果的平均倒数排名、精度或折扣累计收益
    * 需要构建排序评估请求，即 RankEvalRequest，在创建之前需要创建 RankEvalRequest的依赖对象 RankEvalSpec，RankEvalSpec用于描述评估规则，需要定义 RankEvalRequest的计算指标及每个搜索请求的分级文档列表
    * 创建请求时，需要将目标索引名称和 RankEvalSpec 作为参数
    * rate（比率，率），rated（额定，估价），metric（度量标准），Evaluation（评价，评估），Precision（精度），rating（评级，等级评定）
    *  */
    public RankEvalRequest buildRankEvalRequest(String index,String documentId,String field,String content){
        EvaluationMetric metric = new PrecisionAtK();
        List<RatedDocument> ratedDocs = new ArrayList<>();
        //添加 按索引名称、ID和分级指定的分级文档
        ratedDocs.add(new RatedDocument(index,documentId,1));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //创建要评估的搜索查询
        searchSourceBuilder.query(QueryBuilders.matchQuery(field,content));
        //将前三部分合并为 RatedRequest
        RatedRequest ratedRequest = new RatedRequest("content_query",ratedDocs,searchSourceBuilder);
        List<RatedRequest> ratedRequests = Arrays.asList(ratedRequest);
        //创建排序评估规范
        RankEvalSpec specification = new RankEvalSpec(ratedRequests,metric);
        //创建排序评估请求
        RankEvalRequest request = new RankEvalRequest(specification,new String[]{index});
        return request;
    }
    public String executeRankEvalRequest(String index,String documentId,String field,String content,EsUtil esUtil){
        esUtil.initHEs();
        RankEvalRequest request = buildRankEvalRequest(index,documentId,field,content);
        try {
            RankEvalResponse response = esUtil.restHighLevelClient.rankEval(request, RequestOptions.DEFAULT);
            return processRankEvalResponse(response);
        }catch (Exception e){
            e.printStackTrace();
            return "execute rankEval request  error :"+e.getMessage();
        }
        finally {
            esUtil.closeHEs();
        }
    }
    public String processRankEvalResponse(RankEvalResponse response){
        String resultText = "";
        //总体评价结果
        double evaluationResult = response.getMetricScore();
        resultText+="evaluationResult is "+evaluationResult;
        Map<String,EvalQueryQuality> partialResults = response.getPartialResults();
        //获取关键词 content_query 对应的评估结果
        EvalQueryQuality evalQueryQuality = partialResults.get("content_query");
        resultText+="; content_query id is "+evalQueryQuality.getId();
        //每部分结果的度量分数
        double qualityLevel = evalQueryQuality.metricScore();
        resultText+="; qualityLevel is "+qualityLevel;
        List<RatedSearchHit> hitsAndRatings = evalQueryQuality.getHitsAndRatings();
        RatedSearchHit ratedSearchHit = hitsAndRatings.get(2);
        //在分级搜索命中里包含完全成熟的搜索命中 SearchHit
        resultText+="SearchHit id is "+ ratedSearchHit.getSearchHit().getId();
        //分级搜索命中还包含一个可选的 <integer>分级 Optional<Integer>，如果文档在请求中未获得分级，则该分级不存在
        resultText+="; rate's is Present is "+ratedSearchHit.getRating().isPresent();
        MetricDetail metricDetails = evalQueryQuality.getMetricDetails();
        String metricName = metricDetails.getMetricName();
        //度量详细信息，以请求中使用的度量命名
        resultText+="; metricName is "+metricName;
        PrecisionAtK.Detail detail = (PrecisionAtK.Detail)metricDetails;
        //在转换到请求中使用的度量之后，度量详细信息提供了对度量计算部分的深入了解
        resultText+="; detail's relevantRetrieved is "+detail.getRelevantRetrieved();
        resultText+="; detail's retrieved is "+detail.getRetrieved();
        return resultText;
    }




    /* 搜索结果解释
    * 解释API，ExplainAPI，用于为请求和相关的文档计算解释性的分数，无论文档是否匹配这个查询请求，ES都可以给用户提供一些有用的反馈
    * 首先要构建 搜索结果解释请求，创建 ExplainRequest 对象，有两个必选参数 索引名称和文档，同时需要通过 QueryBuilder 来构建查询表达式
    * */
    private ExplainRequest buildExplainRequest(String indexName,String document,String field,String content){
        ExplainRequest request = new ExplainRequest(indexName,document);
        request.query(QueryBuilders.termQuery(field,content));
        //设置路由
        request.routing("routing");
        //使用首选参数，例如执行搜索以首选本地碎片，默认值是在分片之间随机进行的
        request.preference("_local");
        //设置为 真，以检索解释的文档源，还可以通过使用“包含源代码”和“排除源代码”来检索部分文档
        request.fetchSourceContext(new FetchSourceContext(true,new String[]{field},null));
        //允许控制一部分的存储字段（要求在映射中单独存储该字段），并将其返回作为说明文档
        request.storedFields(new String[]{field});
        return request;
    }
    /* 执行搜索结果解释请求，会返回 ExplainResponse 响应
    * ExplainResponse response = restClient.explain(request,RequestOption.DEFAULT);
    * */
    private void processExplainResponse(ExplainResponse response){
        //解释文档的索引名称
        String index = response.getIndex();
        //解释文档的ID
        String id = response.getId();
        //查看解释的文档是否存在
        boolean exists = response.isExists();
        //解释的文档与提供的查询之间是否匹配（匹配是从后台的 Lucene解释中检索的，如果Lucene解释建模匹配，则返回 true，否则返回 false）
        boolean match = response.isMatch();
        //查看是否存在此请求的 Lucene解释
        boolean hasExplanation = response.hasExplanation();
        EsUtil.log.info("match is "+match+"; hasExplanation is "+hasExplanation);
        //获取 Lucene 解释对象（如果存在）
        Explanation explanation = response.getExplanation();
        if(explanation!=null){
            EsUtil.log.info("explanation is "+explanation.toString());
        }
        //如果检索到源或存储字段，则获取 getResult对象
        GetResult getResult = response.getGetResult();
        if(getResult==null)
            return;
        //getResult 内部包含两个映射，用于存储提取的元字段和存储的字段
        //以 Map形式检索
        Map<String,Object> source = getResult.getSource();
        if(source==null)
            return;
        for(String str:source.keySet()){
            EsUtil.log.info("str key is "+str);
        }
        //以映射形式检索指定的存储
        Map<String, DocumentField> fields = getResult.getFields();
        if(fields == null){
            return;
        }
        for(String str:fields.keySet()){
            EsUtil.log.info("field str key is "+str);
        }
    }



    /* 统计
    * 提供了 统计API，即 Count API，用于执行查询请求，并返回与请求匹配的统计结果
    *  */
    public CountRequest buildCountRequest(String index,String routeName,String field,String content){
        //将请求限制为特定名称的索引，设置路由参数，设置 IndiceOptions（控制如何解析不可用索引及如何展开通配符表达式）
        //使用首选参数，例如执行搜索以首选本地分片，默认值是在分片之间随机选择的
        CountRequest request = new CountRequest(index).routing(routeName).indicesOptions(IndicesOptions.lenientExpandOpen()).preference("_local");
        //使用默认选项创建 SearchSourceBuilder
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        //设置查询可以是任意类型的 QueryBuilder
        searchSourceBuilder.query(QueryBuilders.termQuery(field,content));
        //将 SearchSourcebuilder 添加到 CountRequest中
        request.source(searchSourceBuilder);
        return request;
    }
    public void executeCountRequest(String index,String routeName,String field,String content,EsUtil esUtil){
        CountRequest request = buildCountRequest(index,routeName,field,content);
        try {
            CountResponse response = esUtil.restHighLevelClient.count(request,RequestOptions.DEFAULT);
            //统计请求对应的结果命中总数
            long count = response.getCount();
            // HTTP状态代码
            RestStatus status = response.status();
            // 请求是否提前终止
            Boolean terminatedEarly = response.isTerminatedEarly();
            EsUtil.log.info("count is "+count+"; status is "+status.getStatus()+"; terminatedEarly is "+terminatedEarly);
            //与统计请求对应的分片总数
            int totalShards = response.getTotalShards();
            //执行统计请求跳过的分片数量
            int skippedShards = response.getSkippedShards();
            //执行统计请求成功的分片数量
            int successfulShards = response.getSuccessfulShards();
            //执行统计请求失败的分片数量
            int failedShards = response.getFailedShards();
            EsUtil.log.info("totalShards is "+totalShards+"; skippedShards is "+skippedShards+"; successfulShards is "+successfulShards+"; failedShards is "+failedShards);
            //通过遍历 ShardSearchFailures 数组来处理可能的失败信息
            if(response.getShardFailures()==null){
                return;
            }
            for(ShardSearchFailure failure:response.getShardFailures()){
                EsUtil.log.info("fail index is "+failure.index());
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

}
