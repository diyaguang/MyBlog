package com.dygstudio.myblog.service.common;

import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.DetailAnalyzeResponse;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheRequest;
import org.elasticsearch.action.admin.indices.cache.clear.ClearIndicesCacheResponse;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeRequest;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.GetAliasesResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.SyncedFlushResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetIndexResponse;
import org.elasticsearch.client.indices.rollover.RolloverRequest;
import org.elasticsearch.client.indices.rollover.RolloverResponse;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * 索引实战
 * 存储数据的行为就叫做索引，文档会属于一种类型，这些类型会存在于索引中。
 * 索引=》数据库，类型=》表，文档=》行数据，字段=》列数据
 * 索引作为名词时，好比关系数据库中的数据库，存储文档的地方，作为动词时，一个文档存储到索引中。
 * 索引还有倒排索引的意思，文档中的所有字段都会被素银，字段都有一个倒排索引，所有字段都可被索引
 *
 * 索引原理：
 * 文档被索引动作和搜索该文档动作之间是有延迟的，新文档需要在几分钟后方可被搜索到，根本原因在于磁盘
 * ES中，当提交一个新的段到磁盘时，需要执行 fsync操作，确保段被物理地写入磁盘，即使断电也不会被丢失，但不能在每个文档被索引时触发。
 * ES和磁盘间的是文件系统缓存，内存索引缓存中的文档被写入新段的过程消耗资源很低，之后文档会被同步到磁盘，这个过程资源消耗很高，一个文档被缓存，就可以被打开或读取
 * Lucene允许新段在写入后被打开，以便让段中包括的文档被搜索，而不用执行一次全量提交
 * ES 这种写入打开一个新段的轻量级过程，叫做 refresh，默认情况下，每个分片每秒自动刷新一次，这就是认为 ES是近实时搜索，但不是实时搜索的原因
 * 文档的改动不会被立即搜索，但是会在一秒内可见
 *
 * ES为每个文档中的字段分别建立了一个倒排索引，随文档的增加，倒排索引中的词条和词条对应的文档ID列表会不断增大，从而影响ES性能
 * ES采用了 Term Dictionary 和 Trem Index 的方式来简化词条的存储和查找
 * ES 通过增量编码压缩，将大数变为小数，仅存储增量值
 *
 * ES有两种数据刷新方式： Refresh，Flush
 * 索引文档时，文档存储在内存中，默认 1s 进入文件系统缓存。Refresh操作是对 Lucene_index_Reader调用了 ReOpen 操作，此时索引中的数据根性，使用户可以搜索到文档
 * 但是此时还未存储到磁盘上，服务器的宕机，数据丢失，如果要进行持久化，需要调用消耗较大的 Lucene Commit操作。ES频繁刁志勇轻量级的 ReOpen操作来达到近似实时搜索的效果
 * ES 在写入文档时，会写一份 translog日志，可以恢复那些丢失的文档，服务器故障时，保障数据安全
 * Flush可以高效的触发 Lucene Commit，同时清空 translog日志，使数据在 Lucene 层面持久化
 */
public class EsIndex {

    /*
     * IndicesOptions （索引操作选项）
     * 其枚举类型主要定义通配符的作用范围，控制如何处理不可用的具体索引（关闭或丢失）
     * 如何将通配符表达式扩展为实际索引（全部，关闭或打开索引），以及如何处理解析为无索引的通配符表达式。
     */

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
        //所有节点是否已确认请求
        boolean acknowledged = createIndexResponse.isAcknowledged();
        //是否在超时前为索引中的每个分片启动了所需数量的分片副本
        boolean shardsAcknowledged = createIndexResponse.isShardsAcknowledged();
        EsUtil.log.info("acknowledged is "+acknowledged+"; shardsAcknowledged is "+shardsAcknowledged);
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

    public void executeGetIndexRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        GetIndexRequest request = buildGetIndexRequest(index);
        try {
            GetIndexResponse getIndexResponse = esUtil.restHighLevelClient.indices().get(request,RequestOptions.DEFAULT);
            processGetIndexResponse(getIndexResponse,index);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esUtil.closeHEs();
        }
    }
    public void processGetIndexResponse(GetIndexResponse getIndexResponse,String index){
        //检索不同类型的映射到索引的映射元数据 MappingMetadata
        MappingMetaData indexMappings = getIndexResponse.getMappings().get(index);
        if(indexMappings==null)
            return;
        //检索 文档类型和文档属性的映射
        Map<String,Object> indexTypeMappings = indexMappings.getSourceAsMap();
        for(String str : indexTypeMappings.keySet()){
            EsUtil.log.info("key is "+str);
        }
        //获取索引的别名列表
        List<AliasMetaData> indexAliases = getIndexResponse.getAliases().get(index);
        if(indexAliases==null)
            return;
        EsUtil.log.info("indexAliases is "+indexAliases.size());
        //获取为索引设置字符串 index.number_shards 的值，该设置是默认设置的一部分
        //（includeDefault 为 true），如果未显示指定设置，则检索默认设置
        String numberOfShardsString = getIndexResponse.getSetting(index,"index.number_of_shards");
        //检索索引的所有设置
        Settings indexSettings = getIndexResponse.getSettings().get(index);
        //设置对象提供了更多的灵活性，被用来提取作为整数的碎片的设置 index.number
        Integer numberOfShards = indexSettings.getAsInt("index.number_of_shards",null);
        //获取默认设置 index.refresh_interval（includeDefault 默认设置为 true ，如果 includeDefault 设置为 false，则 getIndexResponse.defaultSettings()将返回空映射 ）
        TimeValue time  = getIndexResponse.getDefaultSettings().get(index).getAsTime("index.refresh_interval",null);
        //输出获取到的索引信息
        EsUtil.log.info("numberOfShardsString is "+numberOfShardsString+";indexSettings is "+indexSettings.toString()+";numberOfShards is "+numberOfShards.intValue()+";time is "+time.getMillis());
    }


    /* 删除索引 */
    public DeleteIndexRequest buildDeleteIndexRequest(String index){
        DeleteIndexRequest request = new DeleteIndexRequest(index);

        //配置可选参数
        //等待所有节点删除索引的确认超时时间
        request.timeout(TimeValue.timeValueMinutes(2));  //方式1
        request.timeout("2m");   //方式2
        //从节点连接到主节点的超时时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));  //方式1
        request.masterNodeTimeout("1m");  //方式2
        //设置 IndicesOptions，控制解析不可用索引及展开通配符表达式
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
    /*
     * 功能描述: 删除索引
     * 返回 AcknowledgedResponse 作为相应结果
     * @Param: [index, esUtil]
     * @Return: void
     * @Author: diyaguang
     * @Date: 2020/8/25 10:25
     */
    public void executeDeleteIndexRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        DeleteIndexRequest request = buildDeleteIndexRequest(index);
        try {
            AcknowledgedResponse deleteIndexResponse = esUtil.restHighLevelClient.indices().delete(request,RequestOptions.DEFAULT);
            processAcknowledgedResponse(deleteIndexResponse);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processAcknowledgedResponse(AcknowledgedResponse deleteIndexResponse){
        //所有节点是否已确认请求
        boolean acknowledged = deleteIndexResponse.isAcknowledged();
        EsUtil.log.info("acknowledged is "+acknowledged);
    }


    /* 索引存在验证 */

    public GetIndexRequest buildExistsIndexRequest(String index){
        GetIndexRequest request = new GetIndexRequest(index);
        //配置可选项
        //从主节点返回本地信息或检索状态
        request.local(false);
        //回归到适合人类的格式
        request.humanReadable(true);
        //是否返回每个索引的所有默认设置
        request.includeDefaults(false);
        //控制如何解析不可用索引及如何展开通配符表达式
        //通过实验，设置了下边这个配置后，本来没有的索引，exists 也会返回 true
        //设置 IndicesOptions 去控制不能用的索引如何解决处理，用 lenientExpandOpen 进行通用的处理
        //request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
    public void executeExistsIndexRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        GetIndexRequest request = buildExistsIndexRequest(index);
        try {
            boolean exists = esUtil.restHighLevelClient.indices().exists(request,RequestOptions.DEFAULT);
            EsUtil.log.info("exists is "+exists);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esUtil.closeHEs();
        }
    }


    /* 打开索引
    * 实验证明：如果说索引不存在，则会抛出  IndexNotFoundException 异常 */

    public OpenIndexRequest buildOpenIndexRequest(String index){
        OpenIndexRequest request = new OpenIndexRequest(index);
        //请求的相关的可选的 配置

        //所有节点确认索引打开的超时时间
        request.timeout(TimeValue.timeValueMinutes(2));  //方式一
        //request.timeout("2m");   //方式二
        //从节点连接到主节点的超时时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));  //方式一
        //request.masterNodeTimeout("1m");  //方式二
        //请求返回响应前活跃的分片数量
        request.waitForActiveShards(2);  //方式一
        //request.waitForActiveShards(ActiveShardCount.DEFAULT);  //方式二
        //设置 IndicesOptions
        request.indicesOptions(IndicesOptions.strictExpandOpen());
        return request;
    }
    public String executeOpenIndexRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        OpenIndexRequest request = buildOpenIndexRequest(index);
        try {
            OpenIndexResponse openIndexResponse = esUtil.restHighLevelClient.indices().open(request, RequestOptions.DEFAULT);
            processOpenIndexResponse(openIndexResponse);
            return "Found index:"+index;
        }catch (IndexNotFoundException ex){
            return "no such index:"+index;
        }catch (Exception e){
            e.printStackTrace();
            return "Found error in index:"+index+" :"+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processOpenIndexResponse(OpenIndexResponse openIndexResponse){
        //所有节点是否已确认请求
        boolean acknowledged = openIndexResponse.isAcknowledged();
        //是否在超时前为索引中的每个分片启动了所需数量的分片副本
        boolean shardsAcked = openIndexResponse.isShardsAcknowledged();
        EsUtil.log.info("acknowledged is "+acknowledged+" ; shardsAcked is "+shardsAcked);
    }


    /* 关闭索引
    * 说明：实验证明，在没有该索引的情况下，同样可以执行，没有抛出异常 */
    public CloseIndexRequest buildCloseIndexRequest(String index){
        CloseIndexRequest request = new CloseIndexRequest(index);
        //请求的相关的可选的 配置

        //所有节点确认索引打开的超时时间
        request.timeout(TimeValue.timeValueMinutes(2));  //方式一
        //request.timeout("2m");   //方式二
        //从节点连接到主节点的超时时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));  //方式一
        //request.masterNodeTimeout("1m");  //方式二
        //设置 IndicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
    public String executeCloseIndexRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        CloseIndexRequest request = buildCloseIndexRequest(index);
        try{
            AcknowledgedResponse closeIndexResponse = esUtil.restHighLevelClient.indices().close(request,RequestOptions.DEFAULT);
            //所有节点是否已确认请求
            boolean acknowledged = closeIndexResponse.isAcknowledged();
            EsUtil.log.info(index+" acknowledged is "+acknowledged);
            return acknowledged?"The index is closed":"The index close is failure";
        }catch (Exception e){
            e.printStackTrace();
            return "The index is close error:"+e.getMessage();
        }
    }


    /* 缩小索引
    * 将原索引分片数缩小到一定数量，缩小数量必须为原数量的因子，每个分片的复制都必须在听一个节点内
    * 原理：相同配置创建目标索引，但是主分片数量减少，收缩完后，索引就可以被删除了
    * 收缩条件：1.目标索引必须不存在
    * 2.源索引必须被设置为只读状态
    * 3.当前集群的健康状态为绿色
    * 4.源索引必须比目标索引有更多的主分片数
    * 5.目标索引中的主分片数，必须是源索引的一个因子，可以理解为目标索引的分片数可以被源索引整除
    * 6.源索引的所有索引文档数量如果超过 2147483519 个，则不能够将其收缩为只有一个主分片的目标索引，因为超过了单个分片所能够芳的最大的碎银文档数
    * 7.用于执行索引收缩的节点，必须要有足够的硬盘空间
    * 8.索引收缩前，需要确保当前索引的所有分片（主或副）必须存在于同一个节点前。在执行索引的收缩前，需要先执行分片的移动。
    *
    * 在实验中抛出异常 ElasticsearchStatusException[Elasticsearch exception [type=illegal_argument_exception, reason=the number of source shards [3] must be a multiple of [2]]]
    * 原因如下：目标索引的主分片数设置错误，一定要注意目标索引的主分片数可以被源索引的主分片数给整除，且小于源索引的主分片数。
    * ElasticsearchStatusException[Elasticsearch exception [type=illegal_state_exception, reason=index diyaguang must be read-only to resize index. use "index.blocks.write=true"]]
    * 原因如下：索引被拆分的条件，索引需要被设置为只读
    * */

    public ResizeRequest buildResizeRequest(String sourceIndex,String targetIndex){
        ResizeRequest request = new ResizeRequest(targetIndex,sourceIndex);
        //请求的相关的可选的 配置

        //所有节点确认索引打开的超时时间
        request.timeout(TimeValue.timeValueMinutes(2));  //方式一
        //request.timeout("2m");   //方式二
        //从节点连接到主节点的超时时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));  //方式一
        //request.masterNodeTimeout("1m");  //方式二
        //请求返回响应前活跃的分片数量
        //request.setWaitForActiveShards(2);  //方式一
        request.setWaitForActiveShards(ActiveShardCount.DEFAULT);  //方式二
        //在缩小索引上的目标索引中的分片数、删除从源索引复制的分配要求
        request.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards",1).putNull("index.routing.allocation.require.name"));
        //与目标索引关联的别名
        request.getTargetIndexRequest().alias(new Alias(targetIndex+"_alias"));
        return request;
    }
    public String executeResizeRequest(String sourceIndex,String targetIndex,EsUtil esUtil){
        esUtil.initHEs();
        ResizeRequest request = buildResizeRequest(sourceIndex,targetIndex);
        try{
            ResizeResponse resizeResponse = esUtil.restHighLevelClient.indices().shrink(request,RequestOptions.DEFAULT);
            processResizeResponse(resizeResponse);
            return "index resize successful";
        }catch (Exception e){
            e.printStackTrace();
            return "Index resize failure";
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processResizeResponse(ResizeResponse resizeResponse){
        //所有节点是否已确认请求
        boolean acknowledged = resizeResponse.isAcknowledged();
        //是否在超时前为索引中的每个分片启动了所需数量的分片副本
        boolean shardsAcked = resizeResponse.isShardsAcknowledged();
        EsUtil.log.info("acknowledged is "+acknowledged+" ; shardsAcked is "+shardsAcked);
    }


    /* 拆分索引
    * 索引可以被拆分的次数（以及每个原始分片可以拆分成的分片数）由路由分片的数量设置 index.number_of_routing_shards 的值确定
    * 拆分后的总分片数不能超过该值，酷游分片的数量指定了内部可使用的最大散列空间，以便在具有一致性散列的分片中分发文档。
    * 拆分过程：
    * 1.创建一个新的目标索引，其定义与源相同，但主分片数量比源索引多
    * 2.将源索引中的段硬连接到目标索引
    * 3.创建低级文件后，将再次对所有文档进行哈希处理，以删除不属于当前分片的文档
    * 4.恢复了目标索引，像重新打开原来关闭的索引一样
    * 拆分条件：
    * 1.索引需要被设置为只读
    * 2.当前集群的健康状况为绿色
    * 3.目标索引必须是不存在的
    * 4.源索引的主分片必须小于目标索引的主分片数
    * 5.目标索引的主分片数须是源索引分片数的倍数
    * 6.用于执行拆分的节点必须有足够的空间
    * 实践执行，异常信息：[Elasticsearch exception [type=illegal_argument_exception, reason=the number of source shards [3] must be less that the number of target shards [1]]]
    * 原因：拆分条件 第四条
    * 异常信息：Elasticsearch exception [type=illegal_argument_exception, reason=the number of source shards [3] must be a factor of [5]]
    * 原因：拆分条件 第五条
    * 异常信息： ElasticsearchStatusException[Elasticsearch exception [type=illegal_state_exception, reason=index diyaguang must be read-only to resize index. use "index.blocks.write=true"]]
     * 原因：索引被拆分的条件，索引需要被设置为只读
    * */
    public ResizeRequest buildSplitRequest(String sourceIndex,String targetIndex){
        ResizeRequest request = new ResizeRequest(targetIndex,sourceIndex);
        //使用 ResizeRequest 请求，设置"调整类型"为"拆分"
        request.setResizeType(ResizeType.SPLIT);

        //请求的相关的可选的 配置

        //所有节点确认索引打开的超时时间
        //request.timeout(TimeValue.timeValueMinutes(2));  //方式一
        request.timeout("2m");   //方式二
        //从节点连接到主节点的超时时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));  //方式一
        //request.masterNodeTimeout("1m");  //方式二
        //请求返回响应前活跃的分片数量
        //request.setWaitForActiveShards(2);  //方式一
        request.setWaitForActiveShards(ActiveShardCount.DEFAULT);  //方式二
        //在缩小索引上的目标索引中的分片数、删除从源索引复制的分配要求
        request.getTargetIndexRequest().settings(Settings.builder().put("index.number_of_shards",6).putNull("index.routing.allocation.require.name"));;
        //与目标索引关联的别名
        request.getTargetIndexRequest().alias(new Alias(targetIndex+"_alias"));
        return request;
    }
    public String executeSplitRequest(String sourceIndex,String targetIndex,EsUtil esUtil){
        esUtil.initHEs();
        ResizeRequest request = buildSplitRequest(sourceIndex,targetIndex);
        try{
            ResizeResponse resizeResponse = esUtil.restHighLevelClient.indices().split(request,RequestOptions.DEFAULT);
            processSplitResponse(resizeResponse);
            return "index resize successful";
        }catch (Exception e){
            e.printStackTrace();
            return "Index resize failure";
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processSplitResponse(ResizeResponse resizeResponse){
        //所有节点是否已确认请求
        boolean acknowledged = resizeResponse.isAcknowledged();
        //是否在超时前为索引中的每个分片启动了所需数量的分片副本
        boolean shardsAcked = resizeResponse.isShardsAcknowledged();
        EsUtil.log.info("acknowledged is "+acknowledged+" ; shardsAcked is "+shardsAcked);
    }



    /* 刷新索引 */

    public RefreshRequest buildRefreshRequest(String index){
        //刷新指定的索引
        RefreshRequest request = new RefreshRequest(index);
        //刷新多个索引
        //RefreshRequest requestMultiple = new RefreshRequest(index,index);
        //刷新全部索引
        //RefreshRequest requestAll = new RefreshRequest();

        //可选的配置
        //设置 IndicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
    public String executeRefreshRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        RefreshRequest request = buildRefreshRequest(index);
        try {
            RefreshResponse refreshResponse = esUtil.restHighLevelClient.indices().refresh(request,RequestOptions.DEFAULT);
            processRefreshRsponse(refreshResponse);
            return "The index "+index+" refresh successful";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+index+" refresh failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processRefreshRsponse(RefreshResponse refreshResponse){
        //刷新请求命中的分片总数
        int totalShards = refreshResponse.getTotalShards();
        //刷新成功的分片数
        int successfulShards = refreshResponse.getSuccessfulShards();
        //刷新失败的分片数
        int failedShards = refreshResponse.getFailedShards();
        //在一个或多个分片上刷新失败时的失败列表
        DefaultShardOperationFailedException[] failures = refreshResponse.getShardFailures();
        EsUtil.log.info("totalShards is "+totalShards+"; successfualShards is "+successfulShards+"; failedShards is "+failedShards+"; failures is "+(failures == null?0:failures.length));
    }



    /* Flush 刷新索引
    * 刷新原理：将数据刷新到新索引存储，然后清除内部事务日志释放索引内存。
    * 默认情况下，ES使用内存启发方式根据需要自动触发刷新操作，以清理内存。
    */
    public FlushRequest buildFlushRequest(String index){
        //刷新指定的索引
        FlushRequest request = new FlushRequest(index);
        //刷新多个索引
        //FlushRequest requestMultiple = new FlushRequest(index,index);
        //刷新全部索引
        //FlushRequest requestAll = new FlushRequest();

        //可选的配置
        //设置 IndicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
    public String executeFlushRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        FlushRequest request = buildFlushRequest(index);
        try {
            FlushResponse flushResponse = esUtil.restHighLevelClient.indices().flush(request,RequestOptions.DEFAULT);
            processFlushResponse(flushResponse);
            return "The index "+index+" flush successful";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+index+" flush failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    public void processFlushResponse(FlushResponse flushResponse){
        //刷新请求命中的分片总数
        int totalShards = flushResponse.getTotalShards();
        //刷新成功的分片数
        int successfulShards = flushResponse.getSuccessfulShards();
        //刷新失败的分片数
        int failedShards = flushResponse.getFailedShards();
        //在一个或多个分片上刷新失败时的失败列表
        DefaultShardOperationFailedException[] failures = flushResponse.getShardFailures();
        EsUtil.log.info("totalShards is "+totalShards+"; successfualShards is "+successfulShards+"; failedShards is "+failedShards+"; failures is "+(failures == null?0:failures.length));
    }


    /* 同步 Flush 刷新
    * 原理：ES会跟踪每个分片的索引活动，在5分钟内未收到任何索引操作的分片会自动标记为非活动状态
    * 这样，ES就可以减少分片资源
    * */
    public SyncedFlushRequest buildSyncedFlushRequest(String index){
        //刷新指定的索引
        SyncedFlushRequest request = new SyncedFlushRequest(index);
        //刷新多个索引
        //SyncedFlushRequest requestMultiple = new SyncedFlushRequest(index,index);
        //刷新全部索引
        //SyncedFlushRequest requestAll = new SyncedFlushRequest();

        //可选的配置
        //设置 IndicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
    public String executeSyncFlushRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        SyncedFlushRequest request = buildSyncedFlushRequest(index);
        try {
            SyncedFlushResponse flushResponse = esUtil.restHighLevelClient.indices().flushSynced(request,RequestOptions.DEFAULT);
            processSyncedFlushResponse(flushResponse);
            return "The index "+index+" Syncflush successful";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+index+" Syncflush failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    public void processSyncedFlushResponse(SyncedFlushResponse flushResponse){
        //Flush刷新请求命中的分片总数
        int totalShards = flushResponse.totalShards();
        //Flush刷新成功的分片数
        int successfulShards = flushResponse.successfulShards();
        //Flush刷新失败的分片数
        int failedShards = flushResponse.failedShards();
        EsUtil.log.info("totalShards is "+totalShards+"; successfualShards is "+successfulShards+"; failedShards is "+failedShards+";");
    }



    /* 清除索引缓存 */

    public ClearIndicesCacheRequest buildClearIndicesCacheRequest(String index){
        //清除缓存指定的索引
        ClearIndicesCacheRequest request = new ClearIndicesCacheRequest(index);
        //清除缓存多个索引
        //ClearIndicesCacheRequest requestMultiple = new ClearIndicesCacheRequest(index,index);
        //清除缓存全部索引
        //ClearIndicesCacheRequest requestAll = new ClearIndicesCacheRequest();

        //可选的配置
        //设置 IndicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        return request;
    }
    public String executeClearIndicesCacheRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        ClearIndicesCacheRequest request = buildClearIndicesCacheRequest(index);
        try {
            ClearIndicesCacheResponse flushResponse = esUtil.restHighLevelClient.indices().clearCache(request,RequestOptions.DEFAULT);
            processClearIndicesCacheResponse(flushResponse);
            return "The index "+index+" clearIndexCache successful";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+index+" clearIndexCache failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    public void processClearIndicesCacheResponse(ClearIndicesCacheResponse flushResponse){
        //清除索引请求命中的分片总数
        int totalShards = flushResponse.getTotalShards();
        //清除索引成功的分片数
        int successfulShards = flushResponse.getSuccessfulShards();
        //清除索引失败的分片数
        int failedShards = flushResponse.getFailedShards();
        EsUtil.log.info("totalShards is "+totalShards+"; successfualShards is "+successfulShards+"; failedShards is "+failedShards+";");
    }



    /* 强制合并索引
    * 会合并依赖于 Lucene 索引在每个分片中保存的分段数，强制合并操作通过合并分段来减少分段数量。
    * 如果 Http 连接丢失，则请求将在后台继续执行，并且任何新的请求都会被阻塞 */

    public ForceMergeRequest buildForceMergeIndexRequest(String index){
        //强制合并指定的索引
        ForceMergeRequest request = new ForceMergeRequest(index);
        //强制合并多个索引
        //ForceMergeRequest requestMultiple = new ForceMergeRequest(index1,index2);
        //强制合并全部索引
        //ForceMergeRequest requestAll = new ForceMergeRequest();

        //可选的配置
        //设置 IndicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        //设置 max_num_segments 以控制合并后的段数
        request.maxNumSegments(1);
        //将为已删除标识设置为 true
        request.onlyExpungeDeletes(true);
        //将 flush 标识设置为 true
        request.flush(true);

        return request;
    }
    public String executeForceMergeIndexRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        ForceMergeRequest request = buildForceMergeIndexRequest(index);
        try {
            ForceMergeResponse flushResponse = esUtil.restHighLevelClient.indices().forcemerge(request,RequestOptions.DEFAULT);
            processForceMergeIndexResponse(flushResponse);
            return "The index "+index+" fouceMergeIndex successful";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+index+" fouceMergeIndex failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    public void processForceMergeIndexResponse(ForceMergeResponse forceMergeResponse){
        //强制合并请求命中的分片总数
        int totalShards = forceMergeResponse.getTotalShards();
        //强制合并成功的分片数
        int successfulShards = forceMergeResponse.getSuccessfulShards();
        //强制合并失败的分片数
        int failedShards = forceMergeResponse.getFailedShards();
        //在一个或多个分片上强制合并失败时的失败列表
        DefaultShardOperationFailedException[] failures = forceMergeResponse.getShardFailures();
        EsUtil.log.info("totalShards is "+totalShards+"; successfualShards is "+successfulShards+"; failedShards is "+failedShards+"; failures is "+(failures == null?0:failures.length));
    }



    /* 滚动索引
    * 当索引较大或数据老旧时，可以使用ES提供的滚动索引API，将别名滚动到新的索引
    * */
    public RolloverRequest buildRolloverRequest(String index){
        //指向要滚动的索引别名（第一个参数），以及执行滚动操作时的新索引名称，new index 参数是可选的，可以设置为空
        RolloverRequest request = new RolloverRequest(index,index+"-2");
        //指数年龄
        request.addMaxIndexAgeCondition(new TimeValue(7, TimeUnit.DAYS));
        //索引中的文档数
        request.addMaxIndexDocsCondition(1000);
        //索引的大小
        request.addMaxIndexSizeCondition(new ByteSizeValue(5, ByteSizeUnit.GB));

        //可选参数

        //是否执行滚动
        request.dryRun(true);
        //所有节点确认索引打开的超时时间
        request.setTimeout(TimeValue.timeValueMinutes(2));
        //从节点连接到主节点的超时时间
        request.setMasterTimeout(TimeValue.timeValueMinutes(1));
        //请求返回前等待的活跃分片数量
        request.getCreateIndexRequest().waitForActiveShards(ActiveShardCount.from(2));
        //请求返回前等待的活跃分片数量，重置为默认值
        request.getCreateIndexRequest().waitForActiveShards(ActiveShardCount.DEFAULT);
        //添加应用于新索引的设置，其中包括要为其创建的分片数
        request.getCreateIndexRequest().settings(Settings.builder().put("index.number_of_shards",4));
        //添加与新索引关联的映射
        String mappings = "{\"properties\":{\"field\":{\"type\":\"content\"}}}";
        request.getCreateIndexRequest().mapping(mappings, XContentType.JSON);
        //添加与新索引关联的别名
        request.getCreateIndexRequest().alias(new Alias(index+"-2_alias"));

        return request;
    }
    public String executeRolloverIndexRequest(String index,EsUtil esUtil){
        esUtil.initHEs();
        RolloverRequest request = buildRolloverRequest(index);
        try {
            RolloverResponse rolloverResponse = esUtil.restHighLevelClient.indices().rollover(request,RequestOptions.DEFAULT);
            processrolloverIndexResponse(rolloverResponse);
            return "The index "+index+" rolloverIndex successful";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+index+" rolloverIndex failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    public void processrolloverIndexResponse(RolloverResponse rolloverResponse){

        boolean acknowledged = rolloverResponse.isAcknowledged();
        boolean shardAcked = rolloverResponse.isShardsAcknowledged();
        String oldIndex = rolloverResponse.getOldIndex();
        String newIndex = rolloverResponse.getNewIndex();
        boolean isRolledOver = rolloverResponse.isRolledOver();
        boolean isDryRun = rolloverResponse.isDryRun();
        Map<String,Boolean> conditionStatus = rolloverResponse.getConditionStatus();
        EsUtil.log.info("acknowledged is "+acknowledged+
                "; shardsAcked is "+shardAcked+
                "; oldIndex is "+oldIndex+
                "; newIndex is "+newIndex+
                "; isrolledOver is "+isRolledOver+
                "; isDryRun is "+isDryRun+
                "; conditionStatus size is "+(conditionStatus==null?0:conditionStatus.size()));
    }



    /* 索引别名
    * 为索引别名进行命名，当通过索引别名调用索引时，所有的接口都将自动转换为索引实际名称*/
    public IndicesAliasesRequest buildIndicatesAliasesRqeust(String index,String indexAlias){
        IndicesAliasesRequest request = new IndicesAliasesRequest();
        //创建别名操作，将索引的别名设置为 indexAlias
        //AliasActions支持的操作类型有（ADD，REMOVE，REMOVE_INDEX）
        //要求：索引别名不才能重复，也不能和索引名称重复，用户可以增加，删除别名，但是不能修改
        //AliasActions 还支持配置可选 删选器filter 和可选路由 routing
        IndicesAliasesRequest.AliasActions aliasAction = new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD).index(index).alias(indexAlias);
        //将别名操作添加到请求中
        request.addAliasAction(aliasAction);

        //可选配置参数
        //等待所有节点删除索引的确认超时时间
        request.timeout(TimeValue.timeValueMinutes(2));  //方式1
       // request.timeout("2m");   //方式2
        //从节点连接到主节点的超时时间
        request.masterNodeTimeout(TimeValue.timeValueMinutes(1));  //方式1
        //request.masterNodeTimeout("1m");  //方式2

        return request;
    }
    public String executeIndicesAliasesRequest(String index,String indexAlias,EsUtil esUtil){
        esUtil.initHEs();
        IndicesAliasesRequest request = buildIndicatesAliasesRqeust(index,indexAlias);
        try {
            AcknowledgedResponse rolloverResponse = esUtil.restHighLevelClient.indices().updateAliases(request,RequestOptions.DEFAULT);
            processAcknowledgedResponse(rolloverResponse);
            return "The index "+index+" IndicesAliases successful";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+index+" IndicesAliases failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }



    /* 索引别名存在校验 */
    
    public GetAliasesRequest buildGetAliasesRequest(String indexAlias){
        GetAliasesRequest request = new GetAliasesRequest();
        GetAliasesRequest requestWithAlias = new GetAliasesRequest(indexAlias);
        GetAliasesRequest requestWithAliases = new GetAliasesRequest(new String[]{indexAlias,indexAlias});

        //可选参数设置
        //带校验存在性的别名
        requestWithAlias.aliases(indexAlias);
        //与别名关联的一个或多个索引
        requestWithAlias.indices(indexAlias);
        //本地标志（默认为 false），控制是否需要在本地集群状态或所选主节点持有的集群状态中找别名
        requestWithAlias.local(true);
        //设置 IndicesOptions
        requestWithAlias.indicesOptions(IndicesOptions.lenientExpandOpen());
        return requestWithAlias;
    }

    public String executeGetAliasesRequest(String indexAlias,EsUtil esUtil){
        esUtil.initHEs();
        GetAliasesRequest request = buildGetAliasesRequest(indexAlias);
        try {
            boolean exists = esUtil.restHighLevelClient.indices().existsAlias(request,RequestOptions.DEFAULT);
            return "The index "+indexAlias+" getAliases exists "+exists;
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+indexAlias+" getAliases failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }



    /* 获取索引别名 */

    public String executeGetAliasesRequestForAliases(String indexAlias,EsUtil esUtil){
        esUtil.initHEs();
        GetAliasesRequest request = buildGetAliasesRequest(indexAlias);
        try {
            GetAliasesResponse getAliasesResponse = esUtil.restHighLevelClient.indices().getAlias(request,RequestOptions.DEFAULT);
            processGetAliasesResponse(getAliasesResponse);
            return "The index "+indexAlias+" getAliasesAliases successful ";
        }catch (Exception e){
            e.printStackTrace();
            return "The index "+indexAlias+" getAliasesAliases failure: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processGetAliasesResponse(GetAliasesResponse getAliasesResponse){
        //检索索引及其别名映射
        Map<String, Set<AliasMetaData>> aliases = getAliasesResponse.getAliases();
        if(aliases==null || aliases.size()<=0)
            return;
        //遍历 Map
        Set<Map.Entry<String,Set<AliasMetaData>>> set = aliases.entrySet();
        for(Map.Entry<String,Set<AliasMetaData>> entry : set){
            String key = entry.getKey();
            Set<AliasMetaData> metaSet = entry.getValue();
            if(metaSet==null || metaSet.size()<=0){
                return;
            }
            for(AliasMetaData meta : metaSet){
                String aliass = meta.alias();
                EsUtil.log.info("key is "+key+"; aliaas is "+aliass);
            }
        }
    }
}
