package org.zxp.esclientrhl.demo;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ParsedSum;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.util.JsonUtils;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * @program: 直接使用RestHighLevelClient操作
 * @description: ${description}
 * @author: X-Pacific zhang
 * @create: 2019-01-07 11:45
 **/
public class TestNewClient extends EsclientrhlDemoApplicationTests {
    @Autowired
    RestHighLevelClient client;

    @Test
    public void create_index() {
        CreateIndexRequest request = new CreateIndexRequest("esdemo");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
        );
        request.mapping("main",//类型定义
                "  {\n" +
                        "    \"main\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"proposal_no\": {\n" +
                        "          \"type\": \"keyword\"\n" +
                        "        },\n" +
                        "        \"risk_code\": {\n" +
                        "          \"type\": \"keyword\"\n" +
                        "        },\n" +
                        "        \"risk_name\": {\n" +
                        "          \"type\": \"text\"\n" +
                        "        },\n" +
                        "        \"business_nature\": {\n" +
                        "          \"type\": \"keyword\"\n" +
                        "        },\n" +
                        "        \"business_nature_name\": {\n" +
                        "          \"type\": \"text\"\n" +
                        "        },\n" +
                        "        \"appli_code\": {\n" +
                        "          \"type\": \"keyword\"\n" +
                        "        },\n" +
                        "        \"appli_name\": {\n" +
                        "          \"type\": \"text\",\n" +
                        "          \"fields\": {\"keyword\": {\"type\": \"keyword\",\"ignore_above\": 256}}" +
                        "        },\n" +
                        "        \"insured_code\": {\n" +
                        "          \"type\": \"keyword\"\n" +
                        "        },\n" +
                        "        \"insured_name\": {\n" +
                        "          \"type\": \"keyword\"\n" +
                        "        },\n" +
                        "        \"operate_date\": {\n" +
                        "          \"type\": \"date\"\n" +
                        "        },\n" +
                        "        \"operate_date_format\": {\n" +
                        "          \"type\": \"text\"\n" +
                        "        },\n" +
                        "        \"start_date\": {\n" +
                        "          \"type\": \"date\"\n" +
                        "        },\n" +
                        "        \"end_date\": {\n" +
                        "          \"type\": \"date\"\n" +
                        "        },\n" +
                        "        \"sum_amount\": {\n" +
                        "          \"type\": \"double\"\n" +
                        "        },\n" +
                        "        \"sum_premium\": {\n" +
                        "          \"type\": \"double\"\n" +
                        "        },\n" +
                        "        \"com_code\": {\n" +
                        "          \"type\": \"keyword\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }",//类型映射，需要的是一个JSON字符串
                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request,RequestOptions.DEFAULT);
            //返回的CreateIndexResponse允许检索有关执行的操作的信息，如下所示：
            boolean acknowledged = createIndexResponse.isAcknowledged();//指示是否所有节点都已确认请求
            System.out.println(acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }




//        ActionListener<CreateIndexResponse> listener = new ActionListener<CreateIndexResponse>() {
//            @Override
//            public void onResponse(CreateIndexResponse createIndexResponse) {
//                //如果执行成功，则调用onResponse方法;
//            }
//            @Override
//            public void onFailure(Exception e) {
//                //如果失败，则调用onFailure方法。
//            }
//        };
//        client.indices().createAsync(request, listener);


    }




    @Test
    public void exists_index() throws IOException {
        GetIndexRequest request = new GetIndexRequest();
        request.indices("twitter_two");
        boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
        System.out.println(exists);
    }


    @Test
    public void delete_index() throws IOException {
        DeleteIndexRequest request = new DeleteIndexRequest("esdemo");
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(request, RequestOptions.DEFAULT);
        System.out.println(deleteIndexResponse.isAcknowledged());
    }

    @Test
    public void add_data(){
        IndexRequest indexRequest = new IndexRequest("esdemo", "main");
        IndexDemo m1 = new IndexDemo();
        m1.setProposal_no("111222333aaabbbccc_01");
        m1.setRisk_code("0101");
        m1.setOperate_date(new Date());
        String source = JsonUtils.obj2String(m1);
        indexRequest.source(source, XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("修改成功");
        }
    }


    @Test
    public void add_data_withid(){
        IndexRequest indexRequest = new IndexRequest("esdemo", "main", "111222333aaabbbccc_02");
        IndexDemo m1 = new IndexDemo();
        m1.setProposal_no("111222333aaabbbccc_02");
        m1.setRisk_code("0101");
        m1.setOperate_date(new Date());
        m1.setAppli_name("alli world good");
        String source = JsonUtils.obj2String(m1);
        indexRequest.source(source, XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("修改成功");
        }
    }

    /**
     * 这个是不可以的 （pojo中有_id字段）
     */
    @Test
    public void add_data_with_fieldid(){
        IndexRequest indexRequest = new IndexRequest("esdemo", "main");
        IndexDemo m1 = new IndexDemo();
//        m1.set_id("111222333aaabbbccc_03");
        m1.setProposal_no("111222333aaabbbccc_03");
        m1.setRisk_code("0102");
        m1.setOperate_date(new Date());
        String source = JsonUtils.obj2String(m1);
        indexRequest.source(source, XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("修改成功");
        }
    }


    @Test
    public void update_data(){
        UpdateRequest updateRequest = new UpdateRequest("esdemo", "main", "111222333aaabbbccc_02");

        Map<String, String> map = new HashMap<>();
        map.put("risk_code", "0102");
        updateRequest.doc(map);
        UpdateResponse updateResponse = null;
        try {
            updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("修改成功");
        }
    }

    /**
     * 这样也能修改
     */
    @Test
    public void update_data_byadd(){
        IndexRequest indexRequest = new IndexRequest("esdemo", "main", "111222333aaabbbccc_02");
        IndexDemo m1 = new IndexDemo();
        m1.setProposal_no("111222333aaabbbccc_02");
        m1.setRisk_code("0101");
        m1.setOperate_date(new Date());
        String source = JsonUtils.obj2String(m1);
        indexRequest.source(source, XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
            System.out.println("创建成功");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("修改成功");
        }
    }


    @Test
    public void delete_data(){
        DeleteRequest deleteRequest = new DeleteRequest("esdemo", "main", "111222333aaabbbccc_02");

        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
            System.out.println("删除成功");
        }
    }


    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/current/java-rest-high-search.html
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.5/java-rest-high-query-builders.html
     * @throws IOException
     */
    @Test
    public void search_data() throws IOException {
        SearchRequest searchRequest = new SearchRequest("esdemo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("risk_code","0102"));
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());//这里和以前是一样的
//        searchSourceBuilder.query(QueryBuilders.termQuery("appli_name.keyword","alli world good"));//通过关键字查询
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
//        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));
//        searchRequest.indices("esdemo");
        //指定返回字段begin
//        searchSourceBuilder.fetchSource(false);//关闭source 即不返回字段里的信息了
//        String[] includeFields = new String[] {"proposal_no", "risk_code", "insured_code"}; //只展示配置的字段值
//        String[] excludeFields = new String[] {"insured_code"}; //不展示配置的字段值
//        searchSourceBuilder.fetchSource(includeFields, excludeFields);
        //指定返回字段end
        //高亮高亮研究begin
        HighlightBuilder highlightBuilder = new HighlightBuilder();

        HighlightBuilder.Field highlightTitle = new HighlightBuilder.Field("risk_code");
        HighlightBuilder.Field highlightTitle2 = new HighlightBuilder.Field("risk_name");
//      highlightTitle.highlighterType("unified");
        highlightBuilder.preTags("<font>");
        highlightBuilder.postTags("</font>");
        highlightBuilder.field(highlightTitle);
        highlightBuilder.field(highlightTitle2);
//        HighlightBuilder.Field highlightUser  = new HighlightBuilder.Field("user");
//        highlightBuilder.field(highlightUser);
        searchSourceBuilder.highlighter(highlightBuilder);
        //高亮高亮研究begin

        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits hits = searchResponse.getHits();
        long totalHits = hits.getTotalHits().value;
        float maxScore = hits.getMaxScore();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
//            String index = hit.getIndex();
//            String type = hit.getType();
//            String id = hit.getId();
//            float score = hit.getScore();
//            System.out.println("index:"+index);
//            System.out.println("type:"+type);
//            System.out.println("id:"+id);
//            System.out.println("score:"+score);

//            System.out.println(hit.getSourceAsString());
//            Main main = JsonUtils.string2Obj(hit.getSourceAsString(),Main.class);
//            System.out.println(main);

            System.out.println(hit.getHighlightFields().get("risk_code").fragments()[0].string());//得到高亮字段值
            System.out.println(hit);
        }
    }

    /**
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.5/java-rest-high-aggregation-builders.html
     * https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.5/java-rest-high-search.html
     */
    @Test
    public void aggregation_data() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_risk_code")
                .field("risk_code.keyword");
        aggregation.subAggregation(AggregationBuilders.count("count_proposal_no").field("proposal_no.keyword"));
        searchSourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("esdemo");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        Aggregations aggregations = searchResponse.getAggregations();
        Terms by_risk_code = aggregations.get("by_risk_code");
        for (Terms.Bucket bucket:by_risk_code.getBuckets()){
            ValueCount count = bucket.getAggregations().get("count_proposal_no");
            long value = count.getValue();
            System.out.println(bucket.getKey());
            System.out.println(value);
        }

    }


    @Test
    public void aggregation_data2() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_risk_code")
                .field("risk_code.keyword");
        aggregation.subAggregation(AggregationBuilders.sum("sum_sum_premium").field("sum_premium"));
        searchSourceBuilder.aggregation(aggregation);
        SearchRequest searchRequest = new SearchRequest("esdemo");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Aggregations aggregations = searchResponse.getAggregations();
        Terms by_risk_code = aggregations.get("by_risk_code");
        for (Terms.Bucket bucket:by_risk_code.getBuckets()){
            ParsedSum sum = bucket.getAggregations().get("sum_sum_premium");
            double value = sum.getValue();
            System.out.println(bucket.getKey());
            System.out.println(value);
        }

    }



    @Test
    //配合搜索建议创建索引
    public void create_completion_index() {
        CreateIndexRequest request = new CreateIndexRequest("esdemo2");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
        );
        request.mapping("book",//类型定义
                "  {\n" +
                        "    \"book\": {\n" +
                        "      \"properties\": {\n" +
                        "        \"title\": {\n" +
                        "          \"type\": \"text\",\n" +
                        "          \"fields\": {\n" +
                        "            \"suggest\" : {\n" +
                        "              \"type\" : \"completion\",\n" +
                        "              \"analyzer\": \"ik_max_word\"\n" +
                        "            }\n" +
                        "          }" +
                        "        },\n" +
                        "        \"content\": {\n" +
                        "          \"type\": \"text\",\n" +
                        "          \"analyzer\": \"ik_max_word\"\n" +
                        "        }\n" +
                        "      }\n" +
                        "    }\n" +
                        "  }",//类型映射，需要的是一个JSON字符串
                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request,RequestOptions.DEFAULT);
            //返回的CreateIndexResponse允许检索有关执行的操作的信息，如下所示：
            boolean acknowledged = createIndexResponse.isAcknowledged();//指示是否所有节点都已确认请求
            System.out.println(acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void bulk_data(){
        BulkRequest rrr = new BulkRequest();
        rrr.add(new IndexRequest("esdemo2", "book", "3")
                .source(XContentType.JSON,"title", "1" ,"content" ,"111"));
        rrr.add(new IndexRequest("esdemo2", "book", "4")
                .source(XContentType.JSON,"title", "11","content" ,"1111"));
        rrr.add(new IndexRequest("esdemo2", "book", "5")
                .source(XContentType.JSON,"title", "112233","content" ,"1111"));
        rrr.add(new IndexRequest("esdemo2", "book", "6")
                .source(XContentType.JSON,"title", "123456","content" ,"1111"));
        rrr.add(new IndexRequest("esdemo2", "book", "7")
                .source(XContentType.JSON,"title", "1234123","content" ,"1111"));
        rrr.add(new IndexRequest("esdemo2", "book", "8")
                .source(XContentType.JSON,"title", "111111111","content" ,"1111"));
        try {
            BulkResponse bulkResponse = client.bulk(rrr, RequestOptions.DEFAULT);
            for (BulkItemResponse bulkItemResponse : bulkResponse) {
                DocWriteResponse itemResponse = bulkItemResponse.getResponse();
                if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.INDEX
                        || bulkItemResponse.getOpType() == DocWriteRequest.OpType.CREATE) {
                    IndexResponse indexResponse = (IndexResponse) itemResponse;
                    System.out.println("创建成功");
                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.UPDATE) {
                    UpdateResponse updateResponse = (UpdateResponse) itemResponse;

                } else if (bulkItemResponse.getOpType() == DocWriteRequest.OpType.DELETE) {
                    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * completion_suggest
     * @throws IOException
     */
    @Test
    public void completion_suggest__data() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        CompletionSuggestionBuilder completionSuggestionBuilder = new
                CompletionSuggestionBuilder("title.suggest");
        completionSuggestionBuilder.text("大话");
        completionSuggestionBuilder.size(10);
        suggestBuilder.addSuggestion("suggest_appli_name", completionSuggestionBuilder);
        searchSourceBuilder.suggest(suggestBuilder);

        SearchRequest searchRequest = new SearchRequest("esdemo2");
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        Suggest suggest = searchResponse.getSuggest();
        CompletionSuggestion completionSuggestion = suggest.getSuggestion("suggest_appli_name");
        for (CompletionSuggestion.Entry entry : completionSuggestion.getEntries()) {
            for (CompletionSuggestion.Entry.Option option : entry) {
                String suggestText = option.getText().string();
                System.out.println(suggestText);
            }
        }
    }
}
