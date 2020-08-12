package com.dj.elasticseach;

import com.alibaba.fastjson.JSON;
import com.dj.elasticseach.Pojo.User;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class ElasticseachApplicationTests {
    @Autowired
    @Qualifier("restHighLevelClient")
    private RestHighLevelClient client;

    //索引的创建 Request
    @Test
    void testCerateIndex() throws IOException {
        //1. 创建索引请求
        CreateIndexRequest request = new CreateIndexRequest("joker_index");
        //2. 执行创建请求 indices.create
        CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
        System.out.println(createIndexResponse);
    }

    //获取索引 判断是否存在
    @Test
    void testGetindex() throws IOException {
        GetIndexRequest request = new GetIndexRequest("joker_index");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    //删除索引
    @Test
    void testDeleteindex() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("test2");
        AcknowledgedResponse delete = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.isAcknowledged());
    }
    //添加文档
    @Test
    void testAddDocument() throws IOException {
        User user = new User("joker_dj", 18);
        //创建请求
        IndexRequest request = new IndexRequest("joker_index");
        // 规则 put joker_index/_doc/1
        request.id("2");
        request.timeout(TimeValue.timeValueSeconds(1));
        request.timeout("1s");
        //将我们的数据放入请求 json
        request.source(JSON.toJSONString(user), XContentType.JSON);
        //客户端发送请求
        IndexResponse indexRespons = client.index(request, RequestOptions.DEFAULT);
        System.out.println(indexRespons.toString());
        System.out.println(indexRespons.status());
    }
    //获取文档 判断文档是否存在
    @Test
    void testexitdocument() throws IOException {
        GetRequest request = new GetRequest("joker_index", "1");
        //不获取返回的上下文  _source
        request.fetchSourceContext(new FetchSourceContext(false));
        request.storedFields("_none_");
        boolean exists = client.exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }
    //获取文档的内容
    @Test
    void testGetDocument() throws IOException {
        GetRequest request = new GetRequest("joker_index", "1");
        GetResponse documentFields = client.get(request, RequestOptions.DEFAULT);
        System.out.println(documentFields.toString());//打印文档的内容
    }

    //更新文档信息
    @Test
    void testUpdateDocument() throws IOException {
        UpdateRequest request = new UpdateRequest("joker_index", "1");
        request.timeout("1s");
        User user = new User("joker_djs", 20);
        request.doc(JSON.toJSONString(user),XContentType.JSON);
        UpdateResponse update = client.update(request, RequestOptions.DEFAULT);
        System.out.println(update.status());
    }
    //删除文档记录
    @Test
    void testDeleteRequst() throws IOException {
        DeleteRequest request = new DeleteRequest("joker_index", "1");
        request.timeout("1s");
        DeleteResponse delete = client.delete(request, RequestOptions.DEFAULT);
        System.out.println(delete.status());
    }

    // 批量插入数据
    @Test
    void testBulkRequest() throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("10s");
        ArrayList<User> userList=new ArrayList<>();
        userList.add(new User("joker1",18));
        userList.add(new User("joker2",18));
        userList.add(new User("joker3",18));
        userList.add(new User("joker4",18));
        userList.add(new User("joker5",18));
        userList.add(new User("joker6",18));
        userList.add(new User("joker7",18));
        userList.add(new User("joker8",18));
        for (int i = 0; i < userList.size(); i++) {
            bulkRequest.add(new IndexRequest("joker_index")
                    .id(""+(i+1))
                    .source(JSON.toJSONString(userList.get(i)),XContentType.JSON));
        }
        BulkResponse bulk = client.bulk(bulkRequest, RequestOptions.DEFAULT);
        System.out.println(bulk.hasFailures()); //false 成功
    }
    //查询
    //搜索请求 SearchRequest
    //条件构造 SearchSourceBuilder
    //MatchAllQueryBuilder
    //TermQueryBuilder 精确查询
    // xxx QueryBuilder
    @Test
    void testSearch() throws IOException {
        SearchRequest searchRequest = new SearchRequest("joker_index");
        //构建搜索条件
        SearchSourceBuilder SourceBuilder = new SearchSourceBuilder();
        //高亮
        SourceBuilder.highlighter();
        //查询条件 我们可以使用QueryBuidler
        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name", "joker1");
//        MatchAllQueryBuilder matchAllQueryBuilder = QueryBuilders.matchAllQuery();
        SourceBuilder.query(matchAllQueryBuilder);
        SourceBuilder.timeout(new TimeValue(60,TimeUnit.SECONDS));

        //构建搜索
        searchRequest.source(SourceBuilder);
        /*客户端执行*/
        SearchResponse search = client.search(searchRequest, RequestOptions.DEFAULT);
        System.out.println(JSON.toJSONString(search.getHits()));
        System.out.println("==============================");
        for (SearchHit hit : search.getHits().getHits()) {
            System.out.println(hit.getSourceAsMap());
        }
    }
}
