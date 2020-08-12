package com.dj.jdelasticsearch.Service;

import com.alibaba.fastjson.JSON;
import com.dj.jdelasticsearch.Pojo.Content;
import com.dj.jdelasticsearch.utils.HtmlParseUtil;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
//业务编写
@Service
public class ContentService {
    @Autowired
    private RestHighLevelClient restHighLevelClient;
    //解析数据放入es中
    public boolean parseContent(String keywords) throws IOException {
        List<Content> contents = new HtmlParseUtil().ParseJD(keywords);
        //把查询得到的数据放到es中
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.timeout("2m");
        for (int i = 0; i < contents.size(); i++) {
            bulkRequest.add(new IndexRequest("jd_goods")
                    .source(JSON.toJSONString(contents.get(i)), XContentType.JSON));
        }
        BulkResponse bulk = restHighLevelClient.bulk(bulkRequest, RequestOptions.DEFAULT);
        return  !bulk.hasFailures();
    }

    //2. 获取数据实现搜索功能
    public List<Map<String,Object>> searchPage(String keywords,int page,int pageSize) throws IOException {
        if(page<=1){
            page=1;
        }
        //条件搜索
        SearchRequest searchRequest = new SearchRequest("jd_goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //分页
        sourceBuilder.from(page);
        sourceBuilder.size(pageSize);

       //模糊匹配
        MatchPhraseQueryBuilder title = QueryBuilders.matchPhraseQuery("title", keywords);
        //精确匹配
        //TermQueryBuilder title = QueryBuilders.termQuery("title", keywords);
        sourceBuilder.query(title);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //执行搜索
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //解析结果
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit hit : search.getHits().getHits()) {
            list.add(hit.getSourceAsMap());
        }
        return list;
    }

    //3. 获取数据实现高亮搜索功能
    public List<Map<String,Object>> searchHighlightPage(String keywords,int page,int pageSize) throws IOException {
        if(page<=1){
            page=1;
        }
        //条件搜索
        SearchRequest searchRequest = new SearchRequest("jd_goods");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        //高亮
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.requireFieldMatch(false);//是否多个字段高亮;
        highlightBuilder.field("title");//设置高亮的字段
        highlightBuilder.preTags("<span style='color:red'>");//设置前缀
        highlightBuilder.postTags("</span>");//设置后缀
        sourceBuilder.highlighter(highlightBuilder);
        //分页
        sourceBuilder.from(page);
        sourceBuilder.size(pageSize);

        //模糊匹配
        MatchPhraseQueryBuilder titles = QueryBuilders.matchPhraseQuery("title", keywords);
        //精确匹配
        //TermQueryBuilder title = QueryBuilders.termQuery("title", keywords);
        sourceBuilder.query(titles);
        sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
        //执行搜索
        searchRequest.source(sourceBuilder);
        SearchResponse search = restHighLevelClient.search(searchRequest, RequestOptions.DEFAULT);

        //解析结果
        ArrayList<Map<String,Object>> list = new ArrayList<>();
        for (SearchHit hit : search.getHits().getHits()) {

            Map<String, HighlightField> highlightFields = hit.getHighlightFields();
            HighlightField title = highlightFields.get("title");
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();//原来的结果
            //解析高亮的字段
            if(title!=null){
                Text[] fragments = title.fragments();
                String n_title="";
                for (Text text : fragments) {
                    n_title+=text;
                }
                sourceAsMap.put("title",n_title);
            }
            list.add(sourceAsMap);
        }
        return list;
    }
}
