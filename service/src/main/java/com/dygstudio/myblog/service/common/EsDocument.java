package com.dygstudio.myblog.service.common;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.core.MultiTermVectorsRequest;
import org.elasticsearch.client.core.MultiTermVectorsResponse;
import org.elasticsearch.client.core.TermVectorsRequest;
import org.elasticsearch.client.core.TermVectorsResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.reindex.*;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;

/**
 *  文档操作实战
 *  包括：单文档操作，文档索引，文档获取，完档存在性判断，文档删除，文档更新，词向量，批量处理，多文档获取，重新索引，查询更新，查询删除，多词条向量
 * ES中，存储并索引的 JSON 数据被称为文档，文档有唯一ID进行标识并存储，还包含了元数据。
 * 文档一般包含三个必须的元数据信息：1.index（文档存储的数据结构，即索引），2.type（文档代表的对象类型），3.ID（文档的唯一标识）
 * 索引 index ，类似于“数据库”的概念，标识 ES存储和索引关联数据的数据结构，文档最终被存储和索引在分片中，索引多个分片存储的逻辑空间。
 * 对象类型 type ，类似“表结构” 的概念，每个对象类型都有自己的映射结构，所有类型下的文档都被存储在同一个索引下，type命名有规范，不能包含下划线或逗号
 * 文档唯一标识 ID，ES中创建一个文档时，用户可以自定义id，也可以由ES生成
 * id，type，index 三个元素组合使用时，可以在 ES中唯一标识一个文档
 */
public class EsDocument {

    /* 文档处理过程解析
    * 文档的索引过程：
    * 写入磁盘的倒排索引是不可变的。 1.读写操作轻量级，不需要锁。 2.一旦索引被读入文件系统的内存，就会一直在哪里，不会改变   3.当写入单个大的倒排索引时，ES可以压缩数据
    * 倒排索引的缺点：它不可变，用户不能够改变它。如果要搜索一个新文档，则必须重建整个索引，限制了一个索引能装下的数据，还限制了一个索引可以被更新的频率
    * ES 不是重写整个倒排索引，而是增加额外的索引反映最近的变化，每个倒排索引都可以按顺序查询，从 老旧 的索引开始查询，最后把结果聚合起来。
    * ES底层的 Lucene 中的索引其实是 ES中分分片，ES中的索引是分片的集合，当 ES搜索索引时，它发送查询请求给该索引下的所有分片，然后过滤结果，最后聚合成全局的结果
    * ES 引入了 per-segment search 的概念，一个段（segment）就是一个有完成功能的倒排索引，Lucene中的索引指的是段的集合，再加上提交点（commit point包括所有段的文件）‘
    * 新的文档在被写入磁盘的段之前，需要先写入内存区的索引。
    * 一个 per-segment search 的工作流程：
    * 1.新的文档首先被写入内存区的索引
    * 2.内存那种的索引不断被提交，新段不断产生，但新的提交点产生时，就将这些新段的数据写入磁盘，包括新段的名称
    * 3.新段被打开，于是它包含的文档就可以被检索到
    * 4.内存被清除，等待接收新的文档
    * 当一个请求被接收，所有段依次被查询时，所有段上的 term 统计信息会被聚合，确保每个 term 和 文档的相关性被正确计算。新的文档就能够以较小的代价加入索引。
    * 段是不可变的，意味着文档既不能从旧的段中移除，旧的段中的文档也不能被更新，ES在每一个提交点都引入一个 .del 文件，包含了段上已经被删除的文档
    * 当一个文档被删除时，只是在 .del文件中被标记为删除，文档查询时，被删除的文档依然可以被匹配查询，但是最终返回之前会从结果中删除
    * 当一个文档被更新时，旧版本文档会被标记为删除，新版本的文档在新的段中被索引，较旧的版本会从结果中被删除
    * 被删除的文件越积越多，每个段消耗的如 文件句柄，内存，CPU等资源越来越大，每次搜索请求都需要依次检查每个段，则段越多，查询就越慢。这时，ES 引入了 段合并
    * 段合并的过程中，小段被合并成大段，大段再合并为更大的段，这个过程中不会中断搜索和索引，当新段合并后，即可打开供搜索，而旧段会被删除
    * 合并会消耗很多的 I/O 和 CPU，ES默认情况下，会限制合并过程。ES 还提供了 optimize API ，以便根据需要强制合并段。
    * optimize API 强制分片合并段以达到指定 max_num_segments 参数，这户减少段的数量，达到提高搜索性能的目的，不需要在动态的索引上使用 optimize API，典型场景是记录日志
    *
    * 文档在文件系统中的处理过程：
    * ES 的配置文件 elasticsearch.yml 文件中有个一个配置属性 path.data，该属性包含了 ES中存储的数据的文件夹的路径。如果没有配置，则默认有一个 data目录，默认的 path.data属性的值。
    * ES是使用 Lucene来处理分片级别的索引和插叙你的，data目录中的文件由 ES 和 Lucene 写入。Lucene负责写和维护 Lucene索引文件，ES在 Lucene之上写与功能相关的元数据，如字段映射，索引设置和其他集群元数据等
    * 在 ES配置的 data 目录中，有 nodes文件夹，用于存储本机的节点信息，按照主节点分片的数量，有编号不同的文件夹，通常默认有一个 0的文件夹
    * 在 编号文件夹内，有 node.lock 文件用于确保一次只能从一个数据目录读取，写入一个ES香瓜你的信息，_state 文件夹用于存放当前节点的索引状态信息，indices文件夹用于存放当前节点的索引信息
    * 在 _state 文件夹中，例如有 global-30.st 文件，global前缀表示是一个全局状态文件，.st 扩展名表示是一个包含元数据的状态文件。这是二进制文件，包含有关用户集群的全局元数据，30表示集群元数据的版本
    * 在 indices文件夹中，每个索引有一个随机字符串名字，该文件夹下的索引数量与本机中的索引数量相同
    * 打开一个 索引文件夹，其中包含两类文件夹 _state 和分片文件夹，其中分片文件夹用 数字 0,1,2 等方式标记序号。其中 _state 文件夹中包含了 indices/{index-name}/state/state-{version}.st 路径中的索引状态文件。分片文件夹中包含索引分片相关的数据
    * 打开一个分片文件夹，包含分片相关的状态文件，其中包括版本控制及有关分片是主分片还是副本的信息，_state 文件夹中包含 本分片下的索引信息和 translog日志信息，translog是ES的事务日志，在每个分片 translog目录中的前缀 translog中存在
    * 在 ES 中，事务日志用于确保安全的将数据索引到 ES，无需为每个文档执行低级 Lucene提交，当提交 Lucene索引时，会在 Lucene级别创建一个新的 segment，即执行 fsync()，会产生大量磁盘 I/O
    * 为了能存储索引文档并使其可搜索，而不需要完整的 Lucene提交，ES将其添加到 Lucene IndexWriter，并将其附加到事务日志中。
    *
    * ES 文档分片存储
    * 一个索引一般由多个分片构成，执行 添加，删除，修改 文档操作时，ES需要决定把这个文档存储在那个分片上，这个过程就称为数据路由。
    * ES的路由算法：shard=hash(routing)%number_of_primary_shards
    * 每次对文档进行增删改查的时候，都有一个 routing值，默认是该文档ID的值，随后对这个 routing值使用 Hash函数进行计算，算出的值再和主分片个数取余数，余数的范围永远是 （0~number_of_primary_shards-1 ）之间，文档知道应该存储在那个对应的分片上
    * ES支持手动指定一个值，作为文档ID，手动指定对负载均衡及提升批量读取的性能有一定帮助
    * ES的路由机制导致 索引在建立后不能修改，修改索引主分片数会直接导致路由规则出现严重问题，导致部分数据无法被检索。
    *
    * ES 的数据分区
    * 搜索引擎一般有两种数据分区方式：1.基于文档的分区方式，2.基于词条的分区方式。ES使用的是基于文档的分区方式
    * 基于文档的分区方式 指的是每个文档只存一个分区，每个分区持有整个文档集的一个子集，分区是指一个功能完整的倒排索引
    * 优点：1.每个分区都可以独立的处理查询，2.可以方便添加以文档为单位的索引信息，3.搜索过程中网络开销非常小
    * 基于词条的分区方式：每个分区拥有一部分词条，词条里面包含了与该词条相关的整个 index 的文档数据
    *
    * 乐观锁：
    * ES使用乐观锁来解决数据一致性问题，只需要指定操作的版本即可。当版本号冲突时，ES会提示冲突并排除异常 VersionConflictEngineException，在 ES中，版本号的取值范围是 1到 2(63)-1
    * 乐观锁思想中，认为数据一般不会引发冲突，在数据更新时，才会检测是否存在数据冲突。
    * 乐观锁对应的是 悲观锁，在悲观锁的思想中，认为数据一般会引发冲突，在读数据时写数据操作往往也在进行，因此要在读数据前需要上锁，没有拿到锁的读者或进程只能等待锁的释放。
    * 悲观锁在关系数据库中有大量的应用，行锁，表锁，读锁，写锁，都是悲观锁
    * 在关系数据库中，如果使用 乐观锁，则需要在表中增加一列字段，称为 version，用于记录行数据的版本。每当对数据进行修改操作时，version都+1，修改过程中，会对比 version，相同的才可写入数据
    * 在 Java中，乐观锁思想实现就是 CAS技术（Compare and Swap），多个线程尝试使用 CAS同时更新同一个变量时，只有一个线程能更新成功，其他的线程都会失败，失败的线程并不会被挂起，而是被告知失败，并可再次尝试。
    * CAS操作包含三个操作数，V内存位置，A预期原值，B新值，如果内存位置的值与预期原值相同，则会自动将该位置值更新为新值，否则不做任何操作。
    *  */

    /* 文档索引
    * ES是面向文档的，可以存储整个文档。ES还会索引每个文档的内容使之可以被搜索
    * ES中，可以对文档进行 索引、搜索、排序、过滤等操作
    * 文档索引 API，允许用户将一个类型化的 JSON文档索引到一个特定的索引中，并且使他可以被搜索到。
    * 构建 JSON 文档：
    * 1.手动使用本地 byte[] 或者使用 String 来构建 JSON文档
    * 2.使用 Map，ES会自动把它转换成与其等价的 JSON文档
    * 3.使用 Jackson 这样的第三方类库来序列化 JavaBean，构建 JSON文档
    * 4.使用内置的帮助类 XContentFactory.jsonBuilder 来构建 JSON文档
    * 在 ES内部，每种类型都会被转化成 byte[]，字节数组，JsonBuilder是高度优化过的 JSON生成器，可以直接构建 byte[]
    * 文档通过索引 API被索引后，意味着文档数据可以被存储和搜索，文档通过其 index，type，id 确定其唯一存储所在
    * 在 IndexRequest中，除三个必须参数外，还可以配置可选参数，路由，超时时间，版本，版本类型，索引管道 等配置
    *  */

    //基于 String 构建 IndexRequest
    public IndexRequest buildIndexRequestWithString(String indexName,String document){
        //索引名称
        IndexRequest request = new IndexRequest(indexName);
        //文档 ID
        request.id(document);
        //String 类型文档
        String jsonString = "{\"user\":\"diyaguang\",\"age\":\"37\",\"title\":\"The title is xxxxx\",\"content\":\"这是用来测试的Content\",\"postDate\":\"2019-07-30\",\"message\":\"这个是测试数据 用来测试 Message的。\"}";
        request.source(jsonString, XContentType.JSON);

        //设置路由值
        request.routing("routing");
        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //设置超时策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        //设置版本
        //Validation Failed: 1: create operations only support internal versioning. use index instead;
        //这里指定版本，或者是版本类型，如果是 Create操作，则会抛出上面的错误
        //request.version(2);
        //设置版本类型
        //request.versionType(VersionType.EXTERNAL);
        //设置操作类型
        //The document index error:Elasticsearch exception [type=version_conflict_engine_exception, reason=[testDoc1]: version conflict, document already exists (current version [1])]
        //说明：如果设置了下面的 操作类型，指定了类型，如 Create，那么在操作时，如果文档存在，则就会抛出异常，如果不进行设置，则会根据是否存在文档进行自动的匹配操作类型。
        //request.opType(DocWriteRequest.OpType.CREATE);
        //request.opType("create");
        //在索引文档之前要执行的接收管道的名称
        //试验时抛出的异常：The document index error:Elasticsearch exception [type=illegal_argument_exception, reason=pipeline with id [pipeline] does not exist]
        //原因：执行名称的管道必须存在才可以指定
        //request.setPipeline("pipeline");

        return request;
    }
    //基于 Map构建 IndexRequest
    public void buildIndexRequestWithMap(String indexName,String document) {
        Map<String,Object> jsonMap = new HashMap<>();
        jsonMap.put("user","diyaguang");
        jsonMap.put("postDate","2019-07-30");
        jsonMap.put("message","Hello Elasticsearch");
        IndexRequest indexRequest = new IndexRequest(indexName).id(document).source(jsonMap);
    }
    //基于 XContentBuilder 构建 IndexRequest
    public void buildIndexRequestWithXContentBuilder(String indexName,String document){
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {
                builder.field("user","diyaguang");
                builder.timeField("postDate","2019-07-30");
                builder.field("message","Firefox 无法建立到 localhost:8080 服务器的连接。此站点暂时无法使用或者太过忙碌。" +
                        "请过几分钟后再试。如果您无法载入任何网页，请检查您计算机的网络连接状态。" +
                        "如果您的计算机或网络受到防火墙或者代理服务器的保护，请确认 Firefox 已被授权访问网络。");
            }
            builder.endObject();
            IndexRequest indexRequest = new IndexRequest(indexName).id(document).source(builder);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    //基于键值对构建 IndexRequest
    public void buildIndexRequestWithKV(String indexName,String document){
        IndexRequest indexRequest = new IndexRequest(indexName).id(document).source("user","diyaguang","postDate","2019-07-30","message","Hello Elasticsearch");
    }

    /*
     * 执行索引文档请求
     * 报错：Validation Failed: 1: create operations only support internal versioning. use index instead;
     * 原因：create操作只支持内部版本控制，需要使用外部指定版本，请使用 index
     */
    public String executeIndexRequest(String indexName,String document,EsUtil esUtil){
        esUtil.initHEs();
        IndexRequest request = buildIndexRequestWithString(indexName,document);
        try {
            IndexResponse response = esUtil.restHighLevelClient.index(request, RequestOptions.DEFAULT);
            return processIndexResponse(response);
        }catch (Exception e){
            e.printStackTrace();
            return "The document index error:"+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private String processIndexResponse(IndexResponse response){
        String resultText = "";
        String index = response.getIndex();
        String id  = response.getId();
        EsUtil.log.info("index is "+index+", id is "+id);
        resultText+="index is "+index+", id is "+id;
        if(response.getResult() == DocWriteResponse.Result.CREATED){
            //文档创建时
            EsUtil.log.info("Document is create");
            resultText+=" Document is create";
        }else if(response.getResult() == DocWriteResponse.Result.UPDATED){
            //文档更新是
            EsUtil.log.info("Document has updated!");
            resultText+=" Document has updated!";
        }
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if(shardInfo.getTotal()!=shardInfo.getSuccessful()){
            //处理成功，Shards 小余总 shards 的情况
            EsUtil.log.info("Successed shards ar not enough!");
            resultText+=" Successed shards ar not enough!";
        }
        if(shardInfo.getFailed()>0){
            for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()){
                String reason = failure.reason();
                EsUtil.log.info("Fail reason is "+reason);
                resultText+="Fail reason is "+reason;
            }
        }
        return resultText;
    }


    /* 文档索引查询
    * 需要构建 文档索引查询请求，GetRequest，有两个必选你参数，索引名称和文档ID
    * 在构建请求的过程中，可以选择其他可选参数进行相应的配置，
    * 1.禁用源索引，
    * 2.为特定字段配置源包含关系，
    * 为特定字段配置源配出关系，
    * 为特定存储字段配置检索，
    * 配置路由值，
    * 配置偏好值，
    * 配置在索引文档之前执行刷新，
    * 配置版本号，
    * 配置版本类型
    * */
    public GetRequest buildGetRequest(String indexName,String document){
        GetRequest request = new GetRequest(indexName,document);
        //可选配置
        //禁用源检索，在默认情况下启用，通过实现，设置了这个属性后，文档可以查询到，但是source值返回为 null
        //这样设置后，就不获取返回的 _source 的上下文了
        //request.fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE);
        //为特定字段配置源包含，这样设置只包含指定的字段
        /*String[] includes = new String[]{"message","*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true,includes,excludes);
        request.fetchSourceContext(fetchSourceContext);*/
        //为特定字段配置源排除
        /*includes = Strings.EMPTY_ARRAY;
        excludes = new String[]{"message"};
        fetchSourceContext = new FetchSourceContext(true,includes,excludes);
        request.fetchSourceContext(fetchSourceContext);  //通过试验配置，这个 fetchSourceContext只能按最后的设置为准*/
        //为特定存储字段配置检索
        //通过试验，设置了这个值后，返回 source 的内容就为 null
        //这个参数是关于显示标记为存储在映射中的字段的，默认是关闭的，通常不推荐使用，使用源筛选来选择要返回的原始文档的子集
        //request.storedFields("message");  //还有一种配置 request.storedFields("_none_");
        //说明：这里如果文档在指定的路由下，如果在请求时候不设置路由，则获取不到文档
        request.routing("routing");
        return request;
    }
    public String executeGetRequest(String indexName,String document,EsUtil esUtil){
        esUtil.initHEs();
        GetRequest request = buildGetRequest(indexName,document);
        try {
            GetResponse response = esUtil.restHighLevelClient.get(request,RequestOptions.DEFAULT);
            return processGetResponse(response);
        }catch (Exception e){
            e.printStackTrace();
            return "Get document index error:"+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private String processGetResponse(GetResponse response){
        String index = response.getIndex();
        String id  = response.getId();
        EsUtil.log.info("index is "+index+", id is "+id);
        if(response.isExists()){
            //获取指定字段的值
            //这里的 getField 也与我们想象中的获取文档的字段不同，这里的字段 比如在测试时，返回的是 routing 的值，即路由的这么一个字段。
            String message = "";//response.getField("message").getValue();
            //这里的 get Source 才是获取源字段的内容
            long version = response.getVersion();
            String sourceAsString = response.getSourceAsString();
            Map<String,Object> sourceMap = response.getSourceAsMap();
            byte[] sourceAsBytes = response.getSourceAsBytes();
            EsUtil.log.info("get value is "+message+", version is "+version+", sourceAsString is "+sourceAsString);
            return "get value is "+message+", version is "+version+", sourceAsString is "+sourceAsString;
        }else{
            //当找不到文档时在此处理，尽管返回的响应有 404状态码，但返回的是有效的 getResponse，而不是引发一场
            //这样的响应不包含任何源文档，并且其 isExists 方法返回 false
            return "document is not exists!";
        }
    }

    /* 文档存在性校验
    * 校验文档是否存在于某个索引的接口API，如果被验证的文档存在，则 ExistsAPI 会返回 true，否则会返回 false
    * 这个校验依赖于 GetRequest ，有点像文档查询API，GetAPI
    * 由于返回结果中只包含 布尔值，true 或 false，建议用户关闭提取源和任何存储字段，使请求是轻量级的*/
    public GetRequest buildCheckExistIndexDocumentRequest(String indexName,String document){
        GetRequest request = new GetRequest(indexName,document);
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        //说明：这里如果文档在指定的路由下，如果在请求时候不设置路由，则获取不到文档
        request.routing("routing");
        return request;
    }
    public String executeCheckExistIndexDocumentRequest(String indexName,String document,EsUtil esUtil){
        esUtil.initHEs();
        GetRequest request = buildCheckExistIndexDocumentRequest(indexName,document);
        try {
            boolean exists = esUtil.restHighLevelClient.exists(request,RequestOptions.DEFAULT);
            return "索引："+indexName+" 下的 "+document+" 文档的存在性是 "+exists;
        }catch (Exception e){
            e.printStackTrace();
            return "Check document exists error :"+e.getMessage();
        }
    }



    /* 删除文档索引 */
    public DeleteRequest buildDeleteIndexDocumentsRequest(String indexName,String document){
        DeleteRequest request = new DeleteRequest(indexName,document);
        //可选的配置

        //设置路由值
        //说明：这里如果文档在指定的路由下，如果在请求时候不设置路由，则获取不到文档
        request.routing("routing");
        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //设置超时策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        //设置版本
        //Validation Failed: 1: create operations only support internal versioning. use index instead;
        //这里指定版本，或者是版本类型，如果是 Create操作，则会抛出上面的错误
        //request.version(2);
        //设置版本类型
        //request.versionType(VersionType.EXTERNAL);

        request.routing("routing");
        return request;
    }
    public String executeDeleteIndexDocumentsRequest(String indexName,String document,EsUtil esUtil){
        esUtil.initHEs();
        DeleteRequest request = buildDeleteIndexDocumentsRequest(indexName,document);
        try {
            DeleteResponse response = esUtil.restHighLevelClient.delete(request,RequestOptions.DEFAULT);
            return processDeleteReqeust(response);
        }catch (Exception e){
            e.printStackTrace();
            return "Delete  index document error: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private String processDeleteReqeust(DeleteResponse response){
        String resultText = "";
        String index = response.getIndex();
        String id  = response.getId();
        long version = response.getVersion();
        EsUtil.log.info("delete id is "+id+", index is "+index+", version is "+version);
        resultText+="delete id is "+id+", index is "+index+", version is "+version;
        ReplicationResponse.ShardInfo shardInfo = response.getShardInfo();
        if(shardInfo.getTotal()!=shardInfo.getSuccessful()){
            EsUtil.log.info("Success shards are not enough");
            resultText+=", Success shards are not enough";
        }
        if(shardInfo.getFailed()>0){
            for(ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()){
                String reason = failure.reason();
                EsUtil.log.info("Fail reason is "+reason);
                resultText+="Fail reason is "+reason;
            }
        }
        return resultText;
    }


    /* 更新文档索引
    * 在 ES 的索引中处理文档的 增，删，改 请求是，文档的 version会随着文档改变+1，ES通过使用这个 version来保证所有修改都正确排序。
    * 当一个旧版本出现在新版本之后，就会被简单的忽略
    * Version 这一优点确保数据不会因为修改冲突而丢失，因此用户可以指定文档的 version做想要的更改，如果要修改的版本号不是最新的，则修改请求会失败
    *  */
    public UpdateRequest buildUpdateIndexDocumentRequest(String indexName,String document){
        UpdateRequest request = new UpdateRequest(indexName,document);
        //可选的配置

        //设置路由值
        //说明：这里如果文档在指定的路由下，如果在请求时候不设置路由，则获取不到文档
        request.routing("routing");
        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //设置超时策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        //如果更新的文档在更新时被另一个操作更改，则重试更新操作的次数
        request.retryOnConflict(3);
        //启用源检索，默认情况下禁用
        //request.fetchSource(true);
        //为特定字段配置源包含，这样设置只包含指定的字段
        /*String[] includes = new String[]{"message","*Date"};
        String[] excludes = Strings.EMPTY_ARRAY;
        request.fetchSource(new FetchSourceContext(true,includes,excludes))*/
        //为特定字段配置源排除
        /*includes = Strings.EMPTY_ARRAY;
        excludes = new String[]{"message"};
        request.fetchSource(new FetchSourceContext(true,includes,excludes))*/

        /*还可以使用其他方式构建 UpdateRequest，使用部分文档更新方式，使用部分文档更新方式时，部分文档将与现有文档合并
        * 1.以 JSON方式构建文档
        * 2.以 Map方式构建文档
        * 3.以 XContentBuilder 对象形式提供文档源，ES内置的帮助器自动将其生成为 JSON格式的内容
        * 4.以键值对的形式提供文档源
        * 5.以字符串 String 形式提供文档源
        * */
        request.doc("updated",new Date(),"reason","Year update!");
        //还可以设置，如果被更新的文档不存在，则可以使用 upsert 方法将某些内容定义为新文档
        //可以使用 字符串，Map映射，XContentBuilder 或键值对定义 upsert 文档的内容
        String jsonString = "{\"created\":\"2020-08-27\"}";
        request.upsert(jsonString,XContentType.JSON);

        //执行后说明：1.使用 upsert 时，第一次没有文档的时候，这个 created字段被写入到文档中，并且是创建了新文档（操作为创建）
        //再次执行这个请求后，updated，reason 字段 被更新到这个文档中（操作为更新）
        return request;
    }

    public String executeUpdateIndexDocumentRequest(String indexName,String document,EsUtil esUtil){
        esUtil.initHEs();
        UpdateRequest request = buildUpdateIndexDocumentRequest(indexName,document);
        try {
            UpdateResponse response = esUtil.restHighLevelClient.update(request,RequestOptions.DEFAULT);
            return  processUpdateIndexDocumentRequest(response);
        }catch (Exception e){
            e.printStackTrace();
            return "Update  index document error: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private String processUpdateIndexDocumentRequest(UpdateResponse response){
        String resultText = "";
        String index = response.getIndex();
        String id  = response.getId();
        long version = response.getVersion();
        EsUtil.log.info("update id is "+id+", index is "+index+", version is "+version);
        resultText+="update id is "+id+", index is "+index+", version is "+version;
        if(response.getResult()==DocWriteResponse.Result.CREATED){
            EsUtil.log.info("document is Create");
            resultText+=" document is Create";
        }else  if(response.getResult()==DocWriteResponse.Result.UPDATED){
            EsUtil.log.info("document is Update");
            resultText+=" document is "+response.getResult().toString();
        }else  if(response.getResult()==DocWriteResponse.Result.DELETED){
            EsUtil.log.info("document is Deleted");
            resultText+=" document is Deleted";
        }else  if(response.getResult()==DocWriteResponse.Result.NOOP){
            EsUtil.log.info("document is Noop");
            resultText+=" document is 无操作";
        }
        return resultText;
    }


    /* 获取文档的向量词
    *  Freq（频率）
    *  */
    public TermVectorsRequest buildTermVectorsRequest(String indexName,String document,String field){
        //方式1：遂引种存在的文档
        TermVectorsRequest request = new TermVectorsRequest(indexName,document);
        request.setFields(field);
        //方式2：索引中不存在的文档，可以人工为文档生成词向量
        /*try {
            XContentBuilder docBuilder = XContentFactory.jsonBuilder();
            docBuilder.startObject().field("user","diyaguang").endObject();
            request = new TermVectorsRequest(indexName,docBuilder);
        }catch (Exception e){
            e.printStackTrace();
        }*/

        //可选的配置
        //当把 FieldStatistics 设置为 false（默认为 true）时，可忽略文档计数，文档频率总和，及总术语频率总和
        request.setFieldStatistics(true);
        //将 TermStatistics设置为 true （默认为 false），以显示术语总频率和文档频率
        request.setTermStatistics(true);
        //将 位置 设置为 false（默认为 true），忽略位置的输出
        request.setPositions(true);
        //将 偏移 设置为 false（默认为 true），忽略偏移的输出
        request.setOffsets(true);
        //将 有效载荷 设置为 false（默认为 true），忽略有效载荷的输出
        request.setPayloads(true);
        //设置 FilterSettings，根据 TF-IDF 分数筛选可返回的词条
        Map<String,Integer> filterSettings = new HashMap<>();
        filterSettings.put("max_num_terms",3);
        filterSettings.put("min_term_freq",1);
        filterSettings.put("max_term_freq",10);
        filterSettings.put("min_doc_freq",1);
        filterSettings.put("max_doc_freq",100);
        filterSettings.put("min_word_length",1);
        filterSettings.put("max_word_length",10);
        request.setFilterSettings(filterSettings);
        //设置 PerFieldAnalyzer，指定与字段已有的分词器不同的分析器
        Map<String,String> perFieldAnalyzer = new HashMap<>();
        perFieldAnalyzer.put("user","keyword");
        request.setPerFieldAnalyzer(perFieldAnalyzer);
        //将 Realtime 设置为 false（默认为 true），以便在 Realtime 附近检索术语向量
        request.setRealtime(false);
        //设置路由
        request.setRouting("routing");
        return request;
    }
    public String executeTermVectorRequest(String indexName,String document,String fields,EsUtil esUtil){
        esUtil.initHEs();
        TermVectorsRequest request = buildTermVectorsRequest(indexName,document,fields);
        try {
            TermVectorsResponse response = esUtil.restHighLevelClient.termvectors(request,RequestOptions.DEFAULT);
            return  processTermVectorsResponse(response);
        }catch (Exception e){
            e.printStackTrace();
            return "TermVector  document error: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private String processTermVectorsResponse(TermVectorsResponse response){
        String resultText = "";
        String index = response.getIndex();
        //String type = response.getType();   //已经标记为过时了
        String id  = response.getId();
        //指示是否找到文档
        boolean found = response.getFound();
        resultText+="update id is "+id+", index is "+index+", found is "+found+"\r\n";
        List<TermVectorsResponse.TermVector> list = response.getTermVectorsList();
        resultText+=" list is "+list.size();
        //处理 TermVector
        for(TermVectorsResponse.TermVector tv : list){
            resultText+=processTermVector(tv);
        }
        return resultText;
    }
    private String processTermVector(TermVectorsResponse.TermVector tv){
        String resultText = "";
        String fieldName = tv.getFieldName();
        int docCount = tv.getFieldStatistics().getDocCount();
        long sumTotalTermFreq = tv.getFieldStatistics().getSumTotalTermFreq();
        long sumDocFreq = tv.getFieldStatistics().getSumDocFreq();
        resultText+="fieldName is "+fieldName+"; docCount is "+docCount+"; sumTotalTermFreq is "+sumTotalTermFreq+"; sumDocFreq is "+sumDocFreq+"\r\n";
        if(tv.getTerms() == null){
            return resultText;
        }
        List<TermVectorsResponse.TermVector.Term> terms = tv.getTerms();
        for(TermVectorsResponse.TermVector.Term term : terms){
            String termStr = term.getTerm();
            int termFreq = term.getTermFreq();
            int docFreq = term.getTermFreq();
            long totalTermFreq = term.getTotalTermFreq() == null?0:term.getTotalTermFreq();
            float score = term.getScore()==null?0:term.getScore();
            resultText+="termStr is "+termStr+"; termFreq is "+termFreq+"; docFreq is "+docFreq+"; totalTermFreq is "+totalTermFreq+"; score is "+score+"\r\n";
            if(term.getTokens()!=null){
                List<TermVectorsResponse.TermVector.Token> tokens = term.getTokens();
                for(TermVectorsResponse.TermVector.Token token : tokens){
                    int position = token.getPosition()==null?0:token.getPosition();
                    int startOffset = token.getStartOffset()==null?0:token.getStartOffset();
                    int endOffset = token.getEndOffset()==null?0:token.getEndOffset();
                    String payload = token.getPayload();
                    resultText+="position is "+position+"; startOffset is "+startOffset+"; endOffset is "+endOffset+"; payload is "+payload+"\r\n";
                }
            }
        }
        return resultText;
    }

    /* 批量请求*/
    public BulkRequest buildBulkRequest(String indexName,String field){
        BulkRequest request = new BulkRequest();
        //添加同类型请求
        request.add(new IndexRequest(indexName).id("1").source(XContentType.JSON,field,"xxxxxxx1"));
        request.add(new IndexRequest(indexName).id("2").source(XContentType.JSON,field,"xxxxxxx1"));
        request.add(new IndexRequest(indexName).id("3").source(XContentType.JSON,field,"xxxxxxx1"));
        //添加异形请求
        request.add(new DeleteRequest(indexName,"3"));
        request.add(new UpdateRequest(indexName,"2").doc(XContentType.JSON,field,"xxxxxxxxxxx"));

        //可选的参数配置
        //设置路由值
        //说明：这里如果文档在指定的路由下，如果在请求时候不设置路由，则获取不到文档
        request.routing("routing");
        //设置超时时间
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //设置刷新策略
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        request.setRefreshPolicy("wait_for");
        //设置在继续执行 索引、更新、删除 操作之前必须处于活动状态的分片副本数
        request.waitForActiveShards(2);
        request.waitForActiveShards(ActiveShardCount.ALL);
        //用于所有子请求的全局 pipelineid，即全局管道标识
        return request;
    }
    public String executeBulkRequest(String indexName,String fields,EsUtil esUtil){
        esUtil.initHEs();
        BulkRequest request = buildBulkRequest(indexName,fields);
        try {
            BulkResponse response = esUtil.restHighLevelClient.bulk(request,RequestOptions.DEFAULT);
            processBulkResponse(response);
            return "execute BulkRequest Successful";
        }catch (Exception e){
            e.printStackTrace();
            return "execute BulkRequest  error: "+e.getMessage();
        }finally {
            esUtil.closeHEs();
        }
    }
    private void processBulkResponse(BulkResponse responses){
        if(responses==null){
            EsUtil.log.info( "the bulkResponse is null");
        }
        for(BulkItemResponse bulkItemResponse : responses){
            DocWriteResponse itemResponse = bulkItemResponse.getResponse();
            switch (bulkItemResponse.getOpType()){
                case INDEX:
                    break;
                case CREATE:
                    IndexResponse indexResponse = (IndexResponse)itemResponse;
                    String index = indexResponse.getIndex();
                    String id = indexResponse.getId();
                    long version = indexResponse.getVersion();
                    EsUtil.log.info( "create id is "+id+", index is "+index+", version is "+version);
                    break;
                case UPDATE:
                    UpdateResponse updateResponse = (UpdateResponse)itemResponse;
                    EsUtil.log.info("the bulkResponse is updateResponse");
                    break;
                case DELETE:
                    DeleteResponse deleteResponse = (DeleteResponse)itemResponse;
                    EsUtil.log.info("the bulkResponse is deleteResponse");
                    break;
            }
        }
    }


    /* 批量处理器
    * ES 提供了 BulkProcessor 进行批量操作处理，提供了一个实用程序类，简化批量 Bulk API的使用，允许将索引、更新、删除文档的操作添加到处理器中透明的执行
    * BulkProcess需要依赖如下组件：
    * 1.RestHighLevelClient，用于执行 BulkRequest 和检索 BulkResponse
    * 2.BulkProcessor.Listener，每次执行 BulkRequest 之前和之后，或者当 BulkRequest失败时，都会调用此监视器
    *  */
    public void buildBulkRequestWithBulkProcessor(String indexName,String field,EsUtil esUtil){
        esUtil.initHEs();
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                //批量处理签的动作
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                //批量处理后的动作
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                //批量处理后的动作
            }
        };
        /* 创建批处理器，使用异步方式
        * 还可以根据当前添加的操作数设置刷新批量请求的时间，根据当前添加的操作大小设置刷新批量请求的时间，设置允许执行的并发请求数，设置刷新间隔，以及设置后退策略等
        *  */
        BulkProcessor.Builder builder = BulkProcessor.builder((request,bulkListener)->esUtil.restHighLevelClient.bulkAsync(request,RequestOptions.DEFAULT,bulkListener),listener);
        //可选的配置项

        //根据当前添加的操作数，设置刷新批量请求的时间（默认值为1000，使用-1表示禁用）
        builder.setBulkActions(500);
        //根据当前添加的操作大小，设置刷新批量请求的时间（默认值为 5M，使用-1表示禁用）
        builder.setBulkSize(new ByteSizeValue(1L, ByteSizeUnit.MB));
        //设置允许执行的并发请求数（默认为1，使用0表示仅允许执行单个请求）
        builder.setConcurrentRequests(0);
        //设置刷新间隔（默认为未设置）
        builder.setFlushInterval(TimeValue.timeValueSeconds(10L));
        //设置一个恒定的后退策略，改策略最初等待 1s，最多重试3次
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(1L),3));

        //BulkProcess类提供了一个简单的接口，可以根据请求数量或在指定的时间段后自动的刷新批量操作。
        //在 创建完 BulkProcess 后，用户就可以向其添加请求了
        BulkProcessor processor = builder.build();
        IndexRequest one = new IndexRequest(indexName).id("6").source(XContentType.JSON,"title","xxxxxxxxxxxx1");
        IndexRequest two = new IndexRequest(indexName).id("6").source(XContentType.JSON,"title","xxxxxxxxxxxx2");
        processor.add(one);
        processor.add(two);
        //索引请求将由 BulkProcess 执行，负责每个批量请求调用 BulkProcessor.Listener，配置的监听器提供访问 BulkRequest，BulkResponse的方法。
    }



    /* MultiGet 批量处理实战
    * ES 提供了批量获取 API，合并多个请求，减少每个请求单独处理所需的网路开销
    * 需要创建 MultiGet 请求，初始化为空，需要添加 MultiGetRequest.Item 以配置要提取的内容，MultiGetRequest.Item 有两个必选参数，即索引名称和文档ID。
    * */
    public void buildMultiGetRequest(String indexName,String[] documentIds,EsUtil esUtil){
        if(documentIds==null||documentIds.length<0){
            return;
        }
        MultiGetRequest request = new MultiGetRequest();
        for(String documentId:documentIds){
            request.add(new MultiGetRequest.Item(indexName,documentId));
        }
        //可选的参数配置
        //禁用源检索，默认情况下启用
        request.add(new MultiGetRequest.Item(indexName,documentIds[0]).fetchSourceContext(FetchSourceContext.DO_NOT_FETCH_SOURCE));
        //为特定字段配置源排除（或源包含）关系
        String[] excludes = Strings.EMPTY_ARRAY;
        String[] includes = {"title","content"};
        FetchSourceContext fetchSourceContext = new FetchSourceContext(true,includes,excludes);
        request.add(new MultiGetRequest.Item(indexName,documentIds[0]).fetchSourceContext(fetchSourceContext));
        //配置路由
        request.add(new MultiGetRequest.Item(indexName,documentIds[0]).routing("routing"));
        //配置版本和版本类型
        request.add(new MultiGetRequest.Item(indexName,documentIds[0]).versionType(VersionType.EXTERNAL).version(10123L));
        //配置偏好
        request.preference("title");
        //将实时标志设置为假（默认为真）
        request.realtime(false);
        //在检索文档之前执行刷新(默认为 false)
        request.refresh(true);
        try {
            esUtil.initHEs();
            request.add(new MultiGetRequest.Item(indexName,documentIds[0]).storedFields("title"));
            //简单的结果解析，可以循环处理
            MultiGetResponse response = esUtil.restHighLevelClient.mget(request,RequestOptions.DEFAULT);
            MultiGetItemResponse item = response.getResponses()[0];
            String value = item.getResponse().getField("title").getValue();
            String index = item.getIndex();
            String id = item.getId();
            GetResponse getResponse = item.getResponse();
            if(getResponse.isExists()){
                long version = getResponse.getVersion();
                String sourceAsString = getResponse.getSourceAsString();
                Map<String,Object> sourceAsMap = getResponse.getSourceAsMap();
                byte[] sourceAsBytes = getResponse.getSourceAsBytes();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            esUtil.closeHEs();
        }
        return;
    }



    /* 文档 ReIndex 实战
    * 文档重新索引，用于从一个或更多的索引中复制相关的文档到一个新的索引中进行索引重建。文档 ReIndex请求需要一个现有的源索引和一个可能存在或不存在的目标索引
    * 文档 ReIndex 不尝试设置目标索引，不会复制源索引的设置，用户需要再运行文档 ReIndex操作之前设置目标索引，包括设置映射，分片计数，副本等。
    *  */
    public void buildReIndexReqeust(String fromIndex,String toIndex,EsUtil esUtil){
        ReindexRequest request = new ReindexRequest();
        //添加要从中复制的源的列表
        request.setSourceIndices("source1","source2",fromIndex);
        //添加目标索引
        request.setDestIndex(toIndex);

        /* 可选的配置
        * versionType 的配置可以像索引API一样配置 dest元素来控制乐观并发控制，如果省略 versionType或将其设置为 Internal 会导致 ES忙目的将文档转存到目标中
        * 如果将 versionType 设置为 external，ES会保留源文件中的版本，并更新目标索引中版本比源文件索引中版本旧的所有文档。
        * 当 opType 设置为 cause_reindex时，会在目标索引中创建缺少的文档，所有现有文档都将导致版本冲突，默认个 opType是 index
        *  */

        //设置目标索引的版本类型
        request.setDestVersionType(VersionType.EXTERNAL);
        //设置目标索引的操作类型为创建类型
        request.setDestOpType("create");
        //在默认情况下，版本冲突会中止重新索引进程，我们可以用以下方法计算
        request.setConflicts("proceed");
        //通过添加查询限制文档，下面仅复制用户字段设置为 kimchy 的文档
        request.setSourceQuery(new TermQueryBuilder("user","kimchy"));
        //通过设置大小限制已处理文档的数量
        request.setSize(10);
        //默认情况下，ReIndex使用1000个批次，可以使用 sourceBatchSize 更改批大小
        request.setSourceBatchSize(100);
        //指定管道模式
        request.setDestPipeline("my_pipeline");
        //如果需要用到源索引中的一组特定文档，则需要使用是sort，建议最好选择更具选择性的查询，而不是进行大小和排序
        request.addSortField("field1", SortOrder.DESC);
        request.addSortField("field2", SortOrder.ASC);
        //使用切片滚动对 uid 进行切片，使用 setslices 指定要使用的切片数
        request.setSlices(2);
        //使用 scroll 参数控制 search context 保持活动时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        //设置超时时间
        request.setTimeout(TimeValue.timeValueMinutes(2));
        //调用 reIndex 后，刷新索引
        request.setRefresh(true);

        try {
            BulkByScrollResponse bulkResponse = esUtil.restHighLevelClient.reindex(request,RequestOptions.DEFAULT);
            if(bulkResponse==null)
                return;
            //获取总耗时
            TimeValue timeTaken = bulkResponse.getTook();
            EsUtil.log.info("time is "+timeTaken.getMillis());
            //检查请求是否超时
            boolean timeOut = bulkResponse.isTimedOut();
            //获取已处理文档数
            long totalDocs = bulkResponse.getTotal();
            //获取已更新文档数
            long updateDocs = bulkResponse.getUpdated();
            //获取已创建文档数
            long createdDocs = bulkResponse.getCreated();
            //获取已删除文档数
            long deletedDocs = bulkResponse.getDeleted();
            //已执行的批次数
            long batches = bulkResponse.getBatches();
            //跳过的文档数
            long noops = bulkResponse.getNoops();
            //版本冲突数
            long versionConflicts = bulkResponse.getVersionConflicts();
            //重试批量索引操作的次数
            long bulkRetries = bulkResponse.getBulkRetries();
            //重试索引操作的次数
            long searchRetries = bulkResponse.getSearchRetries();
            //请求阻塞的总时间，不包括当前处于休眠状态的限制时间
            TimeValue throttledMillis = bulkResponse.getStatus().getThrottled();
            EsUtil.log.info("throttledMillis is "+throttledMillis.getMillis());
            //查询失败数量
            List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures();
            //批量操作失败的数量
            List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /* 文档查询时更新实战
    * 在文档查询时更新接口，以便更新索引中的文档，是在不更改源的情况下对索引中的每个文档进行更新，用可以在文档查询时更新接口来修改字段或新增字段
    *  */
    public void buildUpdateByQueryRequest(String indexName,EsUtil esUtil){
        UpdateByQueryRequest request = new UpdateByQueryRequest(indexName);

        //可选配置项
        //在默认情况下，版本冲突会中止重新索引进程，我们可以用以下方法计算
        request.setConflicts("proceed");
        //通过添加查询限制文档，下面仅复制用户字段设置为 kimchy 的文档
        request.setQuery(new TermQueryBuilder("user","diyaguang"));
        //通过设置大小限制已处理文档的数量
        request.setSize(10);
        //默认情况下，ReIndex使用1000个批次，可以使用 sourceBatchSize 更改批大小
        request.setBatchSize(100);
        //指定管道模式
        request.setPipeline("my_pipeline");

        //设置分片滚动来并行化
        request.setSlices(2);
        //使用 scroll 参数控制 search context 保持活动时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        //设置超时时间
        request.setTimeout(TimeValue.timeValueMinutes(2));
        //调用 reIndex 后，刷新索引
        request.setRefresh(true);
        //设置路由，如果提供路由，那么路由将被复制到滚动查询，从而限制于该路由值匹配的分片处理
        request.setRouting("=cat");
        //设置索引选项
        request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        try {
            BulkByScrollResponse bulkResponse = esUtil.restHighLevelClient.updateByQuery(request,RequestOptions.DEFAULT);
            if(bulkResponse==null)
                return;
            //获取总耗时
            TimeValue timeTaken = bulkResponse.getTook();
            EsUtil.log.info("time is "+timeTaken.getMillis());
            //检查请求是否超时
            boolean timeOut = bulkResponse.isTimedOut();
            //获取已处理文档数
            long totalDocs = bulkResponse.getTotal();
            //获取已更新文档数
            long updateDocs = bulkResponse.getUpdated();
            //获取已创建文档数
            long createdDocs = bulkResponse.getCreated();
            //获取已删除文档数
            long deletedDocs = bulkResponse.getDeleted();
            //已执行的批次数
            long batches = bulkResponse.getBatches();
            //跳过的文档数
            long noops = bulkResponse.getNoops();
            //版本冲突数
            long versionConflicts = bulkResponse.getVersionConflicts();
            //重试批量索引操作的次数
            long bulkRetries = bulkResponse.getBulkRetries();
            //重试索引操作的次数
            long searchRetries = bulkResponse.getSearchRetries();
            //请求阻塞的总时间，不包括当前处于休眠状态的限制时间
            TimeValue throttledMillis = bulkResponse.getStatus().getThrottled();
            EsUtil.log.info("throttledMillis is "+throttledMillis.getMillis());
            //查询失败数量
            List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures();
            //批量操作失败的数量
            List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /* 文档查询时删除实战
    * 在查询时删除的接口
    *  */
    public void buildDeleteByQueryRequest(String indexName,EsUtil esUtil){
        DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);

        //可选配置项
        //在默认情况下，版本冲突会中止重新索引进程，我们可以用以下方法计算
        request.setConflicts("proceed");
        //通过添加查询限制文档，下面仅复制用户字段设置为 kimchy 的文档
        request.setQuery(new TermQueryBuilder("user","diyaguang"));
        //通过设置大小限制已处理文档的数量
        request.setSize(10);
        //默认情况下，ReIndex使用1000个批次，可以使用 sourceBatchSize 更改批大小
        request.setBatchSize(100);

        //设置分片滚动来并行化
        request.setSlices(2);
        //使用 scroll 参数控制 search context 保持活动时间
        request.setScroll(TimeValue.timeValueMinutes(10));
        //设置超时时间
        request.setTimeout(TimeValue.timeValueMinutes(2));
        //调用 reIndex 后，刷新索引
        request.setRefresh(true);
        //设置路由，如果提供路由，那么路由将被复制到滚动查询，从而限制于该路由值匹配的分片处理
        request.setRouting("=cat");
        //设置索引选项
        request.setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN);

        try {
            BulkByScrollResponse bulkResponse = esUtil.restHighLevelClient.deleteByQuery(request,RequestOptions.DEFAULT);
            if(bulkResponse==null)
                return;
            //获取总耗时
            TimeValue timeTaken = bulkResponse.getTook();
            EsUtil.log.info("time is "+timeTaken.getMillis());
            //检查请求是否超时
            boolean timeOut = bulkResponse.isTimedOut();
            //获取已处理文档数
            long totalDocs = bulkResponse.getTotal();
            //获取已更新文档数
            long updateDocs = bulkResponse.getUpdated();
            //获取已创建文档数
            long createdDocs = bulkResponse.getCreated();
            //获取已删除文档数
            long deletedDocs = bulkResponse.getDeleted();
            //已执行的批次数
            long batches = bulkResponse.getBatches();
            //跳过的文档数
            long noops = bulkResponse.getNoops();
            //版本冲突数
            long versionConflicts = bulkResponse.getVersionConflicts();
            //重试批量索引操作的次数
            long bulkRetries = bulkResponse.getBulkRetries();
            //重试索引操作的次数
            long searchRetries = bulkResponse.getSearchRetries();
            //请求阻塞的总时间，不包括当前处于休眠状态的限制时间
            TimeValue throttledMillis = bulkResponse.getStatus().getThrottled();
            EsUtil.log.info("throttledMillis is "+throttledMillis.getMillis());
            //查询失败数量
            List<ScrollableHitSource.SearchFailure> searchFailures = bulkResponse.getSearchFailures();
            //批量操作失败的数量
            List<BulkItemResponse.Failure> bulkFailures = bulkResponse.getBulkFailures();
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /* 获取文档索引的多词向量
    * 词向量接口也有批量实现的方式，多词向量接口，允许用户一次获取多个词向量信息
    * 构建多词向量请求，MultitemVectorsRequest，有两种构建方式：
    * 1.创建一个空的 MultiTermVectorsRequest，然后向其添加单个 Term Vectors 请求
    * 2.所有词向量请求共享相同参数时，可以使用所有必要的设置创建模板TermVectorsRequest，并且可以将此模板请求连同执行这些请求的所有文档ID，传递给 MultiTermVectorsRequest 对象
    *  */
    public void buildMultiTermVectorsRequest(String indexName,String[] documentIds,String field,EsUtil esUtil){
        //方法1：创建一个空的 MultiTermVectorsRequest，向其添加单个 Term Vectors请求
        MultiTermVectorsRequest request = new MultiTermVectorsRequest();
        for(String documenId : documentIds){
            TermVectorsRequest tvrequest = new TermVectorsRequest(indexName,documenId);
            tvrequest.setFields(field);
            request.add(tvrequest);
        }
        //方法2：所有词向量请求共享相同参数（如索引和其他设置）
        TermVectorsRequest tvRequestTemplate = new TermVectorsRequest(indexName,"1");
        tvRequestTemplate.setFields(field);
        String[] ids = {"1","2"};
        request = new MultiTermVectorsRequest(ids,tvRequestTemplate);

        try {
            MultiTermVectorsResponse response = esUtil.restHighLevelClient.mtermvectors(request,RequestOptions.DEFAULT);
            if(response == null)
                return;
            List<TermVectorsResponse> tvResponseList = response.getTermVectorsResponses();
            if(tvResponseList == null)
                return;
            for(TermVectorsResponse tvResponse : tvResponseList){
                String id = tvResponse.getId();
                String index = tvResponse.getIndex();
                //等等其他数据设置
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}