package org.zxp.esclientrhl.demo;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.sort.SortBuilders;
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
import java.util.*;

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
     *
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
        elasticsearchTemplate.search(qb, IndexDemo.class).forEach(s -> System.out.println(s));
    }

    /**
     * 测试批量增加
     *
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
     *
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
        IndexDemo mresult = elasticsearchTemplate.getById("zxptestdate", IndexDemo.class);
        System.out.println("返回的Operate_date" + mresult.getOperate_date());
        System.out.println("返回的Operate_date格式化后" + sdf.format(mresult.getOperate_date()));
        System.out.println("返回的Operate_date_format" + mresult.getOperate_date_format());
    }

    /**
     * 修改（按照有值字段更新索引）
     *
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
     *
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
     *
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
     * 根据id删除
     *
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
     *
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
     *
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
     *
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
     *
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
     *
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
        pageList = elasticsearchTemplate.search(QueryBuilders.matchQuery("appli_name", "我"), psh, IndexDemo.class);
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
     *
     * @throws Exception
     */
    @Test
    public void testCount() throws Exception {
        long count = elasticsearchTemplate.count(new MatchAllQueryBuilder(), IndexDemo.class);
        System.out.println(count);
    }

    /**
     * Scroll查询 打镜像 大批量数据查询
     *
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
     *
     * @throws Exception
     */
    @Test
    public void testCompletionSuggest() throws Exception {
        List<String> list = elasticsearchTemplate.completionSuggest("appli_name", "1", IndexDemo.class);
        list.forEach(main2 -> System.out.println(main2));
    }

    /**
     * 根据id查询
     *
     * @throws Exception
     */
    @Test
    public void testSearchByID() throws Exception {
        IndexDemo main2 = elasticsearchTemplate.getById("main2", IndexDemo.class);
        System.out.println(main2);
    }

    /**
     * 批量根据id查询
     *
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
     *
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
     *
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
     *
     * @throws Exception
     */
    @Test
    public void testSQL() throws Exception {
        String result = elasticsearchTemplate.queryBySQL("SELECT * FROM index_demo where proposal_no = '2'", SqlFormat.TXT);
//       String result = elasticsearchTemplate.queryBySQL("SELECT count(*) FROM index ", SqlFormat.TXT);
//       String result = elasticsearchTemplate.queryBySQL("SELECT risk_code,sum(sum_premium) FROM index group by risk_code", SqlFormat.TXT);
        System.out.println(result);
    }

    /**
     * 保存Template
     *
     * @throws Exception
     */
    @Test
    public void testSaveTemplate() throws Exception {
        String templatesource = "{\n" +
                "  \"script\": {\n" +
                "    \"lang\": \"mustache\",\n" +
                "    \"source\": {\n" +
                "      \"_source\": [\n" +
                "        \"proposal_no\",\"appli_name\"\n" +
                "      ],\n" +
                "      \"size\": 20,\n" +
                "      \"query\": {\n" +
                "        \"term\": {\n" +
                "          \"appli_name\": \"{{name}}\"\n" +
                "        }\n" +
                "      }\n" +
                "    }\n" +
                "  }\n" +
                "}";
        elasticsearchTemplate.saveTemplate("tempdemo1", templatesource);
    }

    /**
     * Template方式搜索，Template已经保存在script目录下
     *
     * @throws Exception
     */
    @Test
    public void testSearchTemplate() throws Exception {
        Map param = new HashMap();
        param.put("name", "123");
        elasticsearchTemplate.searchTemplate(param, "tempdemo1", IndexDemo.class).forEach(s -> System.out.println(s));
    }

    /**
     * Template方式搜索，Template内容以参数方式传入
     *
     * @throws Exception
     */
    @Test
    public void testSearchTemplate2() throws Exception {
        Map param = new HashMap();
        param.put("name", "123");
        String templatesource = "{\n" +
                "      \"query\": {\n" +
                "        \"term\": {\n" +
                "          \"appli_name\": \"{{name}}\"\n" +
                "        }\n" +
                "      }\n" +
                "}";
        elasticsearchTemplate.searchTemplateBySource(param, templatesource, IndexDemo.class).forEach(s -> System.out.println(s));
    }

    /**
     * 高级查询，支持更多个性化查询参数的定制——Routing
     * 让数据保持在一个分片中保存
     * @throws Exception
     */
    @Test
    public void testAttachQuery() throws Exception {
        IndexDemo main2 = new IndexDemo();
        main2.setProposal_no("qq360");
        main2.setAppli_name("zzxxpp");
        elasticsearchTemplate.save(main2, "R01");

        Attach attach = new Attach();
        attach.setRouting("R01");
        elasticsearchTemplate.search(QueryBuilders.termQuery("proposal_no", "qq360"), attach, IndexDemo.class)
                .getList().forEach(s -> System.out.println(s));

        IndexDemo main3 = new IndexDemo();
        main3.setProposal_no("qq360");
        main3.setAppli_name("zzxxpp");
        elasticsearchTemplate.save(main3, "R01");
        elasticsearchTemplate.delete(main3,"R01");

    }


    /**
     * 高级查询，支持更多个性化查询参数的定制——pageSortHighLight
     * 让数据保持在一个分片中保存
     * @throws Exception
     */
    @Test
    public void testAttachQuery2() throws Exception {
        PageSortHighLight pageSortHighLight = new PageSortHighLight(1, 5);

        elasticsearchTemplate.search(new MatchAllQueryBuilder(), pageSortHighLight, IndexDemo.class)
                .getList().forEach(s -> System.out.println(s));
    }


    /**
     * 高级查询，支持更多个性化查询参数的定制——Includes
     * 让数据保持在一个分片中保存
     * @throws Exception
     */
    @Test
    public void testAttachQuery3() throws Exception {
        Attach attach = new Attach();
        PageSortHighLight pageSortHighLight = new PageSortHighLight(1, 5);
        attach.setPageSortHighLight(pageSortHighLight);
        String[] ins = {"proposal_no"};
        attach.setIncludes(ins);

        elasticsearchTemplate.search(new MatchAllQueryBuilder(), attach, IndexDemo.class)
                .getList().forEach(s -> System.out.println(s));
    }

    /**
     * 高级查询，支持更多个性化查询参数的定制——sort
     * 让数据保持在一个分片中保存
     * @throws Exception
     */
    @Test
    public void testAttachQuery6() throws Exception {
        Attach attach = new Attach();
        attach.setSearchAfter(true);
        PageSortHighLight pageSortHighLight = new PageSortHighLight(1, 10);
        String sorter = "sum_amount";
        Sort.Order order = new Sort.Order(SortOrder.ASC,sorter);
        pageSortHighLight.setSort(new Sort(order));
        attach.setPageSortHighLight(pageSortHighLight);
        PageList page = elasticsearchTemplate.search(new MatchAllQueryBuilder(),attach,IndexDemo.class);
        page.getList().forEach(s -> System.out.println(s));
        Object[] sortValues = page.getSortValues();
        while (true) {
            attach.setSortValues(sortValues);
            page = elasticsearchTemplate.search(new MatchAllQueryBuilder(),attach,IndexDemo.class);
            if (page.getList() != null && page.getList().size() != 0) {
                page.getList().forEach(s -> System.out.println(s));
                sortValues = page.getSortValues();
            } else {
                break;
            }
        }

    }

    /**
     * 根据查询条件删除
     * @throws Exception
     */
    @Test
    public void testDeleteByCondition() throws Exception {
        IndexDemo main1 = new IndexDemo();
        main1.setProposal_no("main1");
        main1.setInsured_code("123");
        elasticsearchTemplate.save(main1);
        QueryBuilder queryBuilder = QueryBuilders.termQuery("proposal_no", "main1");
        BulkByScrollResponse bulkResponse = elasticsearchTemplate.deleteByCondition(queryBuilder,IndexDemo.class);
        System.out.println(queryBuilder);
    }

    /**
     * script查询
     * @throws Exception
     */
    @Test
    public void testScript() throws Exception {
        Script script = new Script("Math.random()");
        ScriptSortBuilder scriptSortBuilder = SortBuilders.scriptSort(script,ScriptSortBuilder.ScriptSortType.NUMBER).order(SortOrder.DESC);
        SearchRequest searchRequest = new SearchRequest(new String[]{"index"});
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(new MatchAllQueryBuilder());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(10);
        searchSourceBuilder.sort(scriptSortBuilder);
        searchRequest.source(searchSourceBuilder);
    }
}
