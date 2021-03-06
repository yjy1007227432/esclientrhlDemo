package org.zxp.esclientrhl.demo;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
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
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
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
import org.zxp.esclientrhl.demo.domain.ESQueryHelper;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.demo.domain.SearchParam;
import org.zxp.esclientrhl.util.JsonUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @program: ????????????RestHighLevelClient??????
 * @description: ${description}
 * @author: X-Pacific zhang
 * @create: 2019-01-07 11:45
 **/
@Slf4j
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
        request.mapping("_doc","{\"_doc\":{\"properties\":{\"address\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"admincoding\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"cardId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"checkreducestatus\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"city12345code\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"cityId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"comment\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"commentdate\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"commentlable\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"communityId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"communityadmincoding\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"communityname\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"content\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}},\"analyzer\":\"ik_smart\"},\"createTime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"districtId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"doc\":{\"properties\":{\"content\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}},\"file\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"gridId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"gridadmincoding\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"gridname\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"handleendtime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"handleintime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"handlestate\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"handletime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"huifangendtime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"huifangtime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"id\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"incId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"incSource\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isDelayBanJie\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isDelayChuZhi\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isDelayHuiFang\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isDelayQianShou\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isDelayShouLi\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isDelete\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isHuifang\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isInc\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isbmczj\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isfeedback\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"ishandle\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isleaderpermit\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"ispeople\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isrecall\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isselfsolve\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"issend\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"issentry\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"isshouli\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"issign\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"issignremind\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"issolve\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"issupervise\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"istransfer\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"jcjftype\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"latitude\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"linkeventid\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"longitude\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"mobile\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"needvisitflag\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"noDealDate\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"noDealDepartid\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"noDealDepartname\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"noDealReason\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"noDealUser\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"noDealUserid\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"occurdate\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"partybuildingstate\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"placecode\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"platform\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"problemmark\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"recalltime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"recalluser\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"recalluserid\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"reportName\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"secret\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"serialnumber\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"shouliendtime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"shoulitime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"signdepartid\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"signdepartname\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"signendtime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"signintime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"signtime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"signuserid\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"signusername\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"sovleendtime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"sovleintime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"sovletime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"starNum\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"state\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"street\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"streetId\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"unsatisfactoryflag\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"updatetime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"userreporttype\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},\"visitbackintime\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}}}}}",
        XContentType.JSON);
//        request.mapping("main",//????????????
//                "  {\n" +
//                        "    \"main\": {\n" +
//                        "      \"properties\": {\n" +
//                        "        \"proposal_no\": {\n" +
//                        "          \"type\": \"keyword\"\n" +
//                        "        },\n" +
//                        "        \"risk_code\": {\n" +
//                        "          \"type\": \"keyword\"\n" +
//                        "        },\n" +
//                        "        \"risk_name\": {\n" +
//                        "          \"type\": \"text\"\n" +
//                        "        },\n" +
//                        "        \"business_nature\": {\n" +
//                        "          \"type\": \"keyword\"\n" +
//                        "        },\n" +
//                        "        \"business_nature_name\": {\n" +
//                        "          \"type\": \"text\"\n" +
//                        "        },\n" +
//                        "        \"appli_code\": {\n" +
//                        "          \"type\": \"keyword\"\n" +
//                        "        },\n" +
//                        "        \"appli_name\": {\n" +
//                        "          \"type\": \"text\",\n" +
//                        "          \"fields\": {\"keyword\": {\"type\": \"keyword\",\"ignore_above\": 256}}" +
//                        "        },\n" +
//                        "        \"insured_code\": {\n" +
//                        "          \"type\": \"keyword\"\n" +
//                        "        },\n" +
//                        "        \"insured_name\": {\n" +
//                        "          \"type\": \"keyword\"\n" +
//                        "        },\n" +
//                        "        \"operate_date\": {\n" +
//                        "          \"type\": \"date\"\n" +
//                        "        },\n" +
//                        "        \"operate_date_format\": {\n" +
//                        "          \"type\": \"text\"\n" +
//                        "        },\n" +
//                        "        \"start_date\": {\n" +
//                        "          \"type\": \"date\"\n" +
//                        "        },\n" +
//                        "        \"end_date\": {\n" +
//                        "          \"type\": \"date\"\n" +
//                        "        },\n" +
//                        "        \"sum_amount\": {\n" +
//                        "          \"type\": \"double\"\n" +
//                        "        },\n" +
//                        "        \"sum_premium\": {\n" +
//                        "          \"type\": \"double\"\n" +
//                        "        },\n" +
//                        "        \"com_code\": {\n" +
//                        "          \"type\": \"keyword\"\n" +
//                        "        }\n" +
//                        "      }\n" +
//                        "    }\n" +
//                        "  }",//?????????????????????????????????JSON?????????
//                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request,RequestOptions.DEFAULT);
            //?????????CreateIndexResponse????????????????????????????????????????????????????????????
            boolean acknowledged = createIndexResponse.isAcknowledged();//??????????????????????????????????????????
            System.out.println(acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public  void testAggs() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /**
         * ??????tag?????????????????????
         * ??????sum???avg??????????????????
         */
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.
                terms("tag_tr").field("street.keyword").
                subAggregation(AggregationBuilders.count("avg_id").field("latitude.keyword"));; //????????????


        searchSourceBuilder.aggregation(aggregationBuilder);
        /**
         * ?????????????????????
         */
        searchSourceBuilder.size(0);
        /**
         * ??????dsl??????
         */
        log.info("dsl:" + searchSourceBuilder.toString());
        /**
         * ??????????????????????????????
         */
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("xihueventinfo_ik");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        /**
         * ?????????????????????tag_tr????????????????????????
         */
        Aggregations aggregations = response.getAggregations();
        ParsedStringTerms parsedStringTerms = aggregations.get("tag_tr");
        List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
        for (Terms.Bucket bucket : buckets) {
            //key?????????
            String key = bucket.getKey().toString();
            long docCount = bucket.getDocCount();
            //????????????
            Aggregations bucketAggregations = bucket.getAggregations();
//            ParsedSum sumId = bucketAggregations.get("sum_id");
            ParsedValueCount avgId = bucketAggregations.get("avg_id");
//            System.out.println(key + ":" + docCount + "-" + sumId.getValue() + "-" + avgId.getValue());
            System.out.println(key + ":" + docCount + "-"+avgId.getValueAsString());
        }
    }



    @Test
    public  void testMetric() throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /**
         * ??????tag?????????????????????
         * ??????sum???avg??????????????????
         */
        StatsAggregationBuilder aggregationBuilder = AggregationBuilders.stats ("avg_sum_premium").field("sum_premium");

        searchSourceBuilder.aggregation(aggregationBuilder);
        /**
         * ?????????????????????
         */
        searchSourceBuilder.size(0);
        /**
         * ??????dsl??????
         */
        log.info("dsl:" + searchSourceBuilder.toString());
        /**
         * ??????????????????????????????
         */
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("index_demo");
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        /**
         * ?????????????????????tag_tr????????????????????????
         */
        ParsedStats premium = response.getAggregations().get("avg_sum_premium");
        System.out.println(premium.getMax());
    }



    @Test
    public void addAlias() throws IOException {
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index("index_demo")
                        .alias("index_demo4");
        aliasesRequest.addAliasAction(aliasAction);
        AcknowledgedResponse acknowledgedResponse = client.indices().updateAliases(aliasesRequest,RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse);
    }

    @Test
    public void dropAlias() throws IOException {
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                        .index("index_demo")
                        .alias("index_demo2");
        aliasesRequest.addAliasAction(aliasAction);
        AcknowledgedResponse acknowledgedResponse = client.indices().updateAliases(aliasesRequest,RequestOptions.DEFAULT);
        System.out.println(acknowledgedResponse);
    }


    @Test
    public void reindex() throws IOException {
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices("index_demo");
        request.setDestIndex("new_index_demo");
        request.setSourceBatchSize(1000);
        request.setDestOpType("create");
        request.setConflicts("proceed");
//        request.setScroll(TimeValue.timeValueMinutes(10));
//        request.setTimeout(TimeValue.timeValueMinutes(20));
        request.setRefresh(true);
        BulkByScrollResponse response = client.reindex(request, RequestOptions.DEFAULT);
        System.out.println();
    }




    @Test
    public void ESQueryHelperTest() throws IOException {
        List<SearchParam> searchParams = new ArrayList<>();
        SearchParam param = new SearchParam();
        param.setOperator("and");
        param.setColumn("id");
        param.setType("equals");
        param.setVal("000145002cc711ec4bf552f7b566d0de");
        searchParams.add(param);

        String json = JSON.toJSONString(searchParams);



        List<String> list = new ArrayList<>();
        list.add("equal");
        SearchResponse searchResponse = ESQueryHelper.build("xihueventinfo_ik")
                .and(ESQueryHelper.in("id", "0000798ffa0945e1a0b746ca1f632985","000145002cc711ec4bf552f7b566d0de")) // ???????????????
//                .orNew(list)
                .size(10) // ?????????10???
                .execute(client);// ??????????????????

        SearchHit[] searchHits = searchResponse.getHits().getHits();
        List<String> list1 = Arrays.stream(searchHits).map(SearchHit::getSourceAsString).collect(Collectors.toList());
        Long value = searchResponse.getHits().getTotalHits().value;



        System.out.println();
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
            System.out.println("????????????");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("????????????");
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
            System.out.println("????????????");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("????????????");
        }
    }

    /**
     * ????????????????????? ???pojo??????_id?????????
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
            System.out.println("????????????");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("????????????");
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
            System.out.println("????????????");
        } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("????????????");
        }
    }

    /**
     * ??????????????????
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
            System.out.println("????????????");
        } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
            System.out.println("????????????");
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
            System.out.println("????????????");
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
//        searchSourceBuilder.query(QueryBuilders.matchAllQuery());//???????????????????????????
//        searchSourceBuilder.query(QueryBuilders.termQuery("appli_name.keyword","alli world good"));//?????????????????????
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
//        searchSourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC));
        searchSourceBuilder.sort(new FieldSortBuilder("_id").order(SortOrder.ASC));
//        searchRequest.indices("esdemo");
        //??????????????????begin
//        searchSourceBuilder.fetchSource(false);//??????source ?????????????????????????????????
//        String[] includeFields = new String[] {"proposal_no", "risk_code", "insured_code"}; //???????????????????????????
//        String[] excludeFields = new String[] {"insured_code"}; //???????????????????????????
//        searchSourceBuilder.fetchSource(includeFields, excludeFields);
        //??????????????????end
        //??????????????????begin
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
        //??????????????????begin

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

            System.out.println(hit.getHighlightFields().get("risk_code").fragments()[0].string());//?????????????????????
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
    //??????????????????????????????
    public void create_completion_index() {
        CreateIndexRequest request = new CreateIndexRequest("esdemo2");
        request.settings(Settings.builder()
                .put("index.number_of_shards", 3)
                .put("index.number_of_replicas", 0)
        );
        request.mapping("book",//????????????
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
                        "  }",//?????????????????????????????????JSON?????????
                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(request,RequestOptions.DEFAULT);
            //?????????CreateIndexResponse????????????????????????????????????????????????????????????
            boolean acknowledged = createIndexResponse.isAcknowledged();//??????????????????????????????????????????
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
                    System.out.println("????????????");
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
        completionSuggestionBuilder.text("??????");
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
