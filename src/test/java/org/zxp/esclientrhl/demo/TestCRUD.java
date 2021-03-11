package org.zxp.esclientrhl.demo;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.enums.SqlFormat;
import org.zxp.esclientrhl.repository.*;
import org.zxp.esclientrhl.repository.response.ScrollResponse;
import org.zxp.esclientrhl.util.Constant;
import org.zxp.esclientrhl.util.JsonUtils;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

/**
 * @program: esdemo
 * @description: 普通增删改查
 * @author: X-Pacific zhang
 * @create: 2019-02-25 16:47
 **/
public class TestCRUD extends EsclientrhlDemoApplicationTests {
    @Autowired
    ElasticsearchTemplate<IndexDemo, String> elasticsearchTemplate;

    /**
     * 测试保存一条数据
     * @throws Exception
     */
    @Test
    public void testSaveOne() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("main1123123123");
        main1.setAppli_code("123");
        main1.setAppli_name("2");
        main1.setRisk_code("0501");
        main1.setSum_premium(100);
        main1.setOperate_date(new Date());
        elasticsearchTemplate.save(main1);
        QueryBuilder qb = QueryBuilders.termQuery("proposal_no", "main1123123123");
        elasticsearchTemplate.search(qb, IndexDemo.class).forEach(s  -> System.out.println(s));
    }

    /**
     * 测试批量增加
     * @throws Exception
     */
    @Test
    public void testSaveList() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("1111");
        main1.setBusiness_nature_name("J01-002-03");
        IndexDemo main2 = new IndexDemo();
        main2.setProposal_no("2222");
        main2.setBusiness_nature_name("J01-002-04");
        IndexDemo main3 = new IndexDemo();
        main3.setProposal_no("3333");
        main3.setBusiness_nature_name("J01-002-06");
        IndexDemo main4 = new IndexDemo();
        main4.setProposal_no("4444");
        main4.setBusiness_nature_name("J01-002-07");
        elasticsearchTemplate.save(Arrays.asList(main1, main2, main3, main4));
    }

    /**
     * 测试日期类型
     * @throws Exception
     */
    @Test
    public void testDate() throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss");
        Date now = new Date();
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("zxptestdate");
        main1.setOperate_date(now);
        main1.setOperate_date_format(sdf.format(now));
        elasticsearchTemplate.save(main1);
        Thread.sleep(1000);
        IndexDemo mresult = elasticsearchTemplate.getById("zxptestdate",IndexDemo.class);
        System.out.println("返回的Operate_date"+mresult.getOperate_date());
        System.out.println("返回的Operate_date格式化后"+sdf.format(mresult.getOperate_date()));
        System.out.println("返回的Operate_date_format"+mresult.getOperate_date_format());
    }

    /**
     * 修改（按照有值字段更新索引）
     * @throws Exception
     */
    @Test
    public void testUpdate() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("main2");
        main1.setInsured_code("123");
        elasticsearchTemplate.update(main1);
    }

    /**
     * 批量更新（按照有值字段更新索引）
     * @throws Exception
     */
    @Test
    public void testUpdateBatch() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setSum_amount(10000);
        elasticsearchTemplate.batchUpdate(QueryBuilders.matchQuery("appli_name", "123"), main1, IndexDemo.class, 30, true);
        Thread.sleep(9000L);
    }

    /**
     * 覆盖更新索引
     * @throws Exception
     */
    @Test
    public void testCoverUpdate() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("main1");
        main1.setInsured_code("123");
        elasticsearchTemplate.updateCover(main1);
    }

    /**
     *根据id删除
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("main1");
        main1.setInsured_code("123");
        elasticsearchTemplate.delete(main1);
        elasticsearchTemplate.deleteById("main1", IndexDemo.class);
    }

    /**
     * 是否存在（根据id判断）
     * @throws Exception
     */
    @Test
    public void testExists() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("main1");
        main1.setInsured_code("123");
        boolean exists = elasticsearchTemplate.exists("main1", IndexDemo.class);
        System.out.println(exists);
    }

    /**
     * 原始api查询（SearchRequest）
     * @throws Exception
     */
    @Test
    public void testOriSearch() throws Exception {
        SearchRequest searchRequest = new SearchRequest(new String[]{"index_demo"});
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = elasticsearchTemplate.search(searchRequest);

        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            IndexDemo t = JsonUtils.string2Obj(hit.getSourceAsString(), IndexDemo.class);
            System.out.println(t);
        }
    }

    /**
     * 非分页查询，指定最大返回条数
     * @throws Exception
     */
    @Test
    public void testSearchMore() throws Exception {
        List<IndexDemo> main2List = elasticsearchTemplate.searchMore(new MatchAllQueryBuilder(), 7, IndexDemo.class);
        System.out.println(main2List.size());
        main2List.forEach(main2 -> System.out.println(main2));
    }

    /**
     * 原始查询，分页
     */
    @Test
    public void testOriPageSearch() {
        SearchRequest searchRequest = new SearchRequest("index_demo");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(Constant.DEFALT_PAGE_SIZE);
        searchRequest.source(searchSourceBuilder);
    }

    /**
     * 为高级搜索准备数据
     * @throws Exception
     */
    @Test
    public void testSaveForHighlight() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("main1123123123");
        main1.setAppli_code("123");
        main1.setAppli_name("一二三四五唉收到弄得你阿斯达岁的阿斯蒂芬斯蒂芬我单位代缴我佛非我方是的佛挡杀佛第三方东方闪电凡事都红is都if觉得搜房水电费啥都if结算单佛第四届发送到");
        main1.setRisk_code("0501");
        main1.setSum_premium(100);
        elasticsearchTemplate.save(main1);
    }

    /**
     * 高级搜索 分页、高亮、排序
     * @throws Exception
     */
    @Test
    public void testSearchPageSortHighLight() throws Exception {
        int currentPage = 1;
        int pageSize = 10;
        //分页
        PageSortHighLight psh = new PageSortHighLight(currentPage, pageSize);
        //排序
        String sorter = "proposal_no";
        Sort.Order order = new Sort.Order(SortOrder.ASC, sorter);
        psh.setSort(new Sort(order));
        //定制高亮，如果定制了高亮，返回结果会自动替换字段值为高亮内容
        psh.setHighLight(new HighLight().field("appli_name"));
        //可以单独定义高亮的格式
        //new HighLight().setPreTag("<em>");
        //new HighLight().setPostTag("</em>");
        PageList<IndexDemo> pageList = new PageList<>();
        pageList = elasticsearchTemplate.search(QueryBuilders.matchQuery("appli_name","我"), psh, IndexDemo.class);
        pageList.getList().forEach(main2 -> System.out.println(main2));


        //HighLight highLight = new HighLight();
        //highLight.setPreTag("<span style=\"color:red\">");
        //highLight.setPostTag("</span>");
        //highLight.field("appli_code").field("appli_name");
        //
        //psh.setHighLight(highLight);
        //PageList<IndexDemo> pageList = new PageList<>();
        //pageList = elasticsearchTemplate.search(
        //        QueryBuilders.matchQuery("appli_name","中男儿"),
        //        psh, IndexDemo.class);
    }

    /**
     * 查询数量
     * @throws Exception
     */
    @Test
    public void testCount() throws Exception {
        long count = elasticsearchTemplate.count(new MatchAllQueryBuilder(), IndexDemo.class);
        System.out.println(count);
    }

    /**
     * Scroll查询 打镜像 大批量数据查询
     * @throws Exception
     */
    @Test
    public void testScroll() throws Exception {
        //指定scroll镜像保留5小时
        ScrollResponse<IndexDemo> scroll = elasticsearchTemplate.createScroll(new MatchAllQueryBuilder(), IndexDemo.class, 5L, 10000);
        scroll.getList().forEach(System.out::println);
        ScrollResponse<IndexDemo> scrollResponse = elasticsearchTemplate.queryScroll(IndexDemo.class, 5L, scroll.getScrollId());
        scrollResponse.getList().forEach(System.out::println);

        //清除scroll
        ClearScrollResponse clearScrollResponse = elasticsearchTemplate.clearScroll(scroll.getScrollId());
        System.out.println(clearScrollResponse.isSucceeded());
    }

    /**
     * completionSuggest
     * @throws Exception
     */
    @Test
    public void testCompletionSuggest() throws Exception {
        List<String> list = elasticsearchTemplate.completionSuggest("appli_name", "1", IndexDemo.class);
        list.forEach(main2 -> System.out.println(main2));
    }

    /**
     * 根据id查询
     * @throws Exception
     */
    @Test
    public void testSearchByID() throws Exception {
        IndexDemo main2 = elasticsearchTemplate.getById("main2", IndexDemo.class);
        System.out.println(main2);
    }

    /**
     * 批量根据id查询
     * @throws Exception
     */
    @Test
    public void testMGET() throws Exception {
        String[] list = {"main2", "main3"};
        List<IndexDemo> listResult = elasticsearchTemplate.mgetById(list, IndexDemo.class);
        listResult.forEach(main -> System.out.println(main));
    }

    /**
     * 更新索引集合（分批方式，提升性能，防止es服务内存溢出，每批默认5000条数据）
     * @throws Exception
     */
    @Test
    public void testBatchSveAndUpdate() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("aaa");
        main1.setBusiness_nature_name("aaaaaa2");
        IndexDemo main2 = new IndexDemo();
        main2.setProposal_no("bbb");
        main2.setBusiness_nature_name("aaaaaa2");
        IndexDemo main3 = new IndexDemo();
        main3.setProposal_no("ccc");
        main3.setBusiness_nature_name("aaaaaa2");
        IndexDemo main4 = new IndexDemo();
        main4.setProposal_no("ddd");
        main4.setBusiness_nature_name("aaaaaa2");
        main4.setCom_code("aa");
        elasticsearchTemplate.bulkUpdateBatch(Arrays.asList(main1, main2, main3, main4));
    }

    /**
     * 通过uri querystring进行查询
     * @throws Exception
     */
    @Test
    public void testURI() throws Exception {
        //"q=sum_premium:100"查询sum_premium为100的结果
        List<IndexDemo> list = elasticsearchTemplate.searchUri("q=proposal_no:2", IndexDemo.class);
        list.forEach(s -> System.out.println(s));
    }

    /**
     * 通过sql进行查询
     * @throws Exception
     */
    @Test
    public void testSQL() throws Exception {
        String result = elasticsearchTemplate.queryBySQL("SELECT * FROM index_demo where proposal_no = '2'", SqlFormat.TXT);
//       String result = elasticsearchTemplate.queryBySQL("SELECT count(*) FROM index ", SqlFormat.TXT);
//       String result = elasticsearchTemplate.queryBySQL("SELECT risk_code,sum(sum_premium) FROM index group by risk_code", SqlFormat.TXT);
        System.out.println(result);
    }
//
//    @Test
//    public void testSaveTemplate() throws Exception {
//        String templatesource = "{\n" +
//                "  \"script\": {\n" +
//                "    \"lang\": \"mustache\",\n" +
//                "    \"source\": {\n" +
//                "      \"_source\": [\n" +
//                "        \"proposal_no\",\"appli_name\"\n" +
//                "      ],\n" +
//                "      \"size\": 20,\n" +
//                "      \"query\": {\n" +
//                "        \"term\": {\n" +
//                "          \"appli_name\": \"{{name}}\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "    }\n" +
//                "  }\n" +
//                "}";
//        elasticsearchTemplate.saveTemplate("tempdemo1", templatesource);
//    }
//
//    @Test
//    public void testSearchTemplate() throws Exception {
//        Map param = new HashMap();
//        param.put("name", "123");
//        elasticsearchTemplate.searchTemplate(param, "tempdemo1", Main2.class).forEach(s -> System.out.println(s));
//    }
//
//    @Test
//    public void testSearchTemplate2() throws Exception {
//        Map param = new HashMap();
//        param.put("name", "123");
//        String templatesource = "{\n" +
//                "      \"query\": {\n" +
//                "        \"term\": {\n" +
//                "          \"appli_name\": \"{{name}}\"\n" +
//                "        }\n" +
//                "      }\n" +
//                "}";
//        elasticsearchTemplate.searchTemplateBySource(param, templatesource, Main2.class).forEach(s -> System.out.println(s));
//    }
//
//
//    @Test
//    public void testQueryBuilder() throws Exception {
////        QueryBuilder queryBuilder = QueryBuilders.termQuery("appli_name.keyword","456");
//        //中国好男儿
////        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("appli_name","中国");
////        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("appli_name","中男").slop(1);
////        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("sum_premium").from(1).to(3);
//
////        QueryBuilder queryBuilder = QueryBuilders.matchQuery("appli_name","中男儿");
////        QueryBuilder queryBuilder = QueryBuilders.matchQuery("appli_name","spting");
////        ((MatchQueryBuilder) queryBuilder).fuzziness(Fuzziness.AUTO);
////        QueryBuilder queryBuilder = QueryBuilders.matchQuery("appli_name","spring sps").operator(Operator.AND);
////        QueryBuilder queryBuilder = QueryBuilders.matchQuery("appli_name","中 男 儿 美 丽 人 生").minimumShouldMatch("75%");
////        QueryBuilder queryBuilder = QueryBuilders.fuzzyQuery("appli_name","spting");
//
////        QueryBuilder queryBuilder1 = QueryBuilders.termQuery("appli_name.keyword","spring").boost(5);
////        QueryBuilder queryBuilder2 = QueryBuilders.termQuery("appli_name.keyword","456").boost(3);
////        QueryBuilder queryBuilder3 = QueryBuilders.termQuery("appli_name.keyword","123");
////        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
////        queryBuilder.should(queryBuilder1).should(queryBuilder2).should(queryBuilder3);
//
////        QueryBuilder queryBuilder = QueryBuilders.prefixQuery("appli_name","1");
////        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("appli_name","1?3");
////        QueryBuilder queryBuilder = QueryBuilders.regexpQuery("appli_name","[0-9].+");
//
////        QueryBuilder queryBuilder1 = QueryBuilders.termQuery("appli_name.keyword","spring");
////        QueryBuilder queryBuilder2 = QueryBuilders.termQuery("appli_name.keyword","456");
////        QueryBuilder queryBuilder3 = QueryBuilders.termQuery("risk_code","0101");
////        QueryBuilder queryBuilder4 = QueryBuilders.termQuery("proposal_no.keyword","1234567");
////        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
////        queryBuilder.should(queryBuilder1).should(queryBuilder2);
////        queryBuilder.must(queryBuilder3);
////        queryBuilder.mustNot(queryBuilder4);
//
////        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("appli_name.keyword","456"))
////                .filter(QueryBuilders.matchPhraseQuery("risk_code","0101"));
//
////        QueryBuilders.disMaxQuery()
////                .add(QueryBuilders.matchQuery("title", "bryant fox"))
////                .add(QueryBuilders.matchQuery("body", "bryant fox"))
////                .tieBreaker(0.2f);
////
////        QueryBuilders.multiMatchQuery("Quick pets", "title","body")
////                .minimumShouldMatch("20%")
////                .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
////                .tieBreaker(0.2f);
////
////        QueryBuilders.multiMatchQuery("shanxi datong", "s1","s2","s3","s4")
////                .type(MultiMatchQueryBuilder.Type.MOST_FIELDS);
////
////
////        QueryBuilders.multiMatchQuery("chengdu sichuan", "s1","s2","s3","s4")
////                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);
////
////        //新的算分 = 老的算分 * log( 1 + factor*votes的值)
////        ScoreFunctionBuilder<?> scoreFunctionBuilder = ScoreFunctionBuilders
////                .fieldValueFactorFunction("votes")
////                .modifier(FieldValueFactorFunction.Modifier.LOG1P)
////                .factor(0.1f);
////        QueryBuilders.functionScoreQuery(QueryBuilders.matchQuery("title", "bryant fox"),scoreFunctionBuilder)
////                .boostMode(CombineFunction.MULTIPLY)//默认就是乘
////                .maxBoost(3f);
//
//
//        QueryBuilder queryBuilder = QueryBuilders.boostingQuery(QueryBuilders.matchQuery("title", "bryant fox"),
//                QueryBuilders.matchQuery("flag", "123")).negativeBoost(0.2f);
//
//        List<Main2> list = elasticsearchTemplate.search(queryBuilder, Main2.class);
//        list.forEach(main2 -> System.out.println(main2));
//    }
//
//
//    @Test
//    public void testAttachQuery() throws Exception {
//        Main2 main2 = new Main2();
//        main2.setProposal_no("qq360");
//        main2.setAppli_name("zzxxpp");
//        elasticsearchTemplate.save(main2, "R01");
//
//        Attach attach = new Attach();
//        attach.setRouting("R01");
//        elasticsearchTemplate.search(QueryBuilders.termQuery("proposal_no", "qq360"), attach, Main2.class)
//                .getList().forEach(s -> System.out.println(s));
//
//    }
//
//
//    @Test
//    public void testAttachQuery2() throws Exception {
//        Attach attach = new Attach();
//        elasticsearchTemplate.search(QueryBuilders.termQuery("proposal_no", "qq360"), attach, Main2.class)
//                .getList().forEach(s -> System.out.println(s));
//    }
//
//    @Test
//    public void testAttachQuery3() throws Exception {
//        Main2 main2 = new Main2();
//        main2.setProposal_no("qq360");
//        main2.setAppli_name("zzxxpp");
////        elasticsearchTemplate.delete(main2,"R02");
//
////        elasticsearchTemplate.delete(main2);
//
//        elasticsearchTemplate.save(main2, "R01");
//        elasticsearchTemplate.delete(main2,"R01");
//    }
//
//    @Test
//    public void testAttachQuery4() throws Exception {
//        Attach attach = new Attach();
//        PageSortHighLight pageSortHighLight = new PageSortHighLight(1, 5);
//        attach.setPageSortHighLight(pageSortHighLight);
//        String[] ins = {"proposal_no"};
//        attach.setIncludes(ins);
//
//        elasticsearchTemplate.search(new MatchAllQueryBuilder(), attach, Main2.class)
//                .getList().forEach(s -> System.out.println(s));
//    }
//
//    @Test
//    public void testAttachQuery5() throws Exception {
//        PageSortHighLight pageSortHighLight = new PageSortHighLight(1, 5);
//
//        elasticsearchTemplate.search(new MatchAllQueryBuilder(), pageSortHighLight, Main2.class)
//                .getList().forEach(s -> System.out.println(s));
//    }
//
//
//    @Test
//    public void testAttachQuery6() throws Exception {
//        Attach attach = new Attach();
//        attach.setSearchAfter(true);
//        PageSortHighLight pageSortHighLight = new PageSortHighLight(1, 10);
//        String sorter = "sum_amount";
//        Sort.Order order = new Sort.Order(SortOrder.ASC,sorter);
//        pageSortHighLight.setSort(new Sort(order));
//        attach.setPageSortHighLight(pageSortHighLight);
//        PageList page = elasticsearchTemplate.search(new MatchAllQueryBuilder(),attach,Main2.class);
//        page.getList().forEach(s -> System.out.println(s));
//        Object[] sortValues = page.getSortValues();
//        while (true) {
//            attach.setSortValues(sortValues);
//            page = elasticsearchTemplate.search(new MatchAllQueryBuilder(),attach,Main2.class);
//            if (page.getList() != null && page.getList().size() != 0) {
//                page.getList().forEach(s -> System.out.println(s));
//                sortValues = page.getSortValues();
//            } else {
//                break;
//            }
//        }
//
//    }
//
//
//
//    @Test
//    public void testAttachQuery7() throws Exception {
//        Attach attach = new Attach();
//        PageSortHighLight pageSortHighLight = new PageSortHighLight(2, 12);
//        String sorter = "proposal_no.keyword";
//        Sort.Order order = new Sort.Order(SortOrder.ASC,sorter);
//        pageSortHighLight.setSort(new Sort(order));
//        attach.setPageSortHighLight(pageSortHighLight);
//        PageList page = elasticsearchTemplate.search(new MatchAllQueryBuilder(),attach,Main2.class);
//        page.getList().forEach(s -> System.out.println(s));
//    }
//
//    @Test
//    public void testNewScroll() throws Exception {
//        ScrollResponse<Main2> scrollResponse = elasticsearchTemplate.createScroll(new MatchAllQueryBuilder(), Main2.class, 1L, 100);
//        scrollResponse.getList().forEach(s -> System.out.println(s));
//        String scrollId = scrollResponse.getScrollId();
//        while (true){
//            scrollResponse = elasticsearchTemplate.queryScroll(Main2.class, 1L, scrollId);
//            if(scrollResponse.getList() != null && scrollResponse.getList().size() != 0){
//                scrollResponse.getList().forEach(s -> System.out.println(s));
//                scrollId = scrollResponse.getScrollId();
//            }else{
//                break;
//            }
//        }
//    }
//
//    @Test
//    public void testQueryID() throws Exception {
////        Main2 main2 = new Main2();
////        main2.setProposal_no("qq360_01");
////        main2.setAppli_name("zzxxpp_01");
////        elasticsearchTemplate.save(main2);
////
////        Main2 main3 = new Main2();
////        main3.setAppli_name("zzxxpp_01");
////        elasticsearchTemplate.save(main3);
//
//        Attach attach = new Attach();
//        elasticsearchTemplate.searchMore(new MatchAllQueryBuilder(), 100, Main2.class)
//                .forEach(s -> System.out.println(s));
//
//    }
//
//
//    @Test
//    public void testDeleteByCondition() throws Exception {
//        Main2 main1 = new Main2();
//        main1.setProposal_no("main1");
//        main1.setInsured_code("123");
//        elasticsearchTemplate.save(main1);
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("proposal_no", "main1");
//
//        BulkByScrollResponse bulkResponse = elasticsearchTemplate.deleteByCondition(queryBuilder,Main2.class);
//        System.out.println(queryBuilder);
//    }
//
//    @Test
//    public void testFunctionscoreBool() throws Exception {
//        ScoreFunctionBuilder<?> scoreFunctionBuilder = ScoreFunctionBuilders
//                .fieldValueFactorFunction("votes")
//                .modifier(FieldValueFactorFunction.Modifier.LOG1P)
//                .factor(0.1f);
//        QueryBuilder fsqb = QueryBuilders.functionScoreQuery(QueryBuilders.matchQuery("title", "bryant fox"),scoreFunctionBuilder)
//                .boostMode(CombineFunction.MULTIPLY)//默认就是乘
//                .maxBoost(3f);
//
//        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("appli_name.keyword","456"))
//        .must(fsqb);
//        List<Main2> list = elasticsearchTemplate.search(queryBuilder,Main2.class);
//        list.forEach(main2 -> System.out.println(main2));
//    }
//
//
//
//    @Autowired
//    ElasticsearchTemplate<EsProduct, Long> elasticsearchTemplate5;
//
//    @Test
//    public void testLongTest() throws Exception {
////        LongTest longTest = new LongTest();
////        longTest.setMyid(12345678L);
////        longTest.setMessage("你好");
////        longTest.setTitle("你好");
////        elasticsearchTemplate5.save(longTest);
//        System.out.println("*******");
//        QueryBuilder queryBuilder = QueryBuilders.termQuery("myid", 12345678L);
//        PageSortHighLight psh = new PageSortHighLight(1, 10);
//        String sorter = "sale";
////        String sorter = "id";
//        Sort.Order order = new Sort.Order(SortOrder.ASC,sorter);
//        psh.setSort(new Sort(order));
//        elasticsearchTemplate5.search(new MatchAllQueryBuilder(), psh, EsProduct.class).getList().forEach(s -> System.out.println(s));
////        elasticsearchTemplate5.completionSuggest("title", "你", LongTest.class).forEach(s -> System.out.println(s));
//    }
//
//    @Autowired
//    ElasticsearchTemplate<Main10,String> elasticsearchTemplate10;
//    @Test
//    public void testType() throws Exception {
//        //Main10 main10 = new Main10();
//        //main10.setA("a");
//        //main10.setB("b");
//        //main10.setC(3);
//        //main10.setD(4);
//        //main10.setE(5.55d);
//        //main10.setF(6.66d);
//        //main10.setG(77777777777l);
//        //main10.setH(888888888888l);
//        //main10.setI(new BigDecimal(99999999.99999));
//        //main10.setJ(new Date());
//        //main10.setK(false);
//        //elasticsearchTemplate10.save(main10);
//        //Thread.sleep(1000);
//
//
//        QueryBuilder qb = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("a", "a"))
//                .must(QueryBuilders.matchQuery("b", "b"));
//        elasticsearchTemplate10.search(qb, Main10.class).forEach(System.out::print);
//
//
//        Thread.sleep(10000000);
//    }

}
