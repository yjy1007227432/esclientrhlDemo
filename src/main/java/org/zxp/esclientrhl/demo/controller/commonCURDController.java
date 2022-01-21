package org.zxp.esclientrhl.demo.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.google.common.base.Joiner;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import org.zxp.esclientrhl.demo.domain.*;
import org.zxp.esclientrhl.demo.enums.Constants;
import org.zxp.esclientrhl.demo.response.SqlResponse;
import org.zxp.esclientrhl.demo.service.ElasticsearchTemplateNew;
import org.zxp.esclientrhl.demo.util.ParseSearchParamUtil;
import org.zxp.esclientrhl.enums.SqlFormat;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;
import org.zxp.esclientrhl.repository.PageList;
import org.zxp.esclientrhl.util.JsonUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@Api(tags = "es常用操作")
@Slf4j
@Validated
public class commonCURDController {
    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private ElasticsearchTemplateNew elasticsearchTemplateNew;


    /**
     * 创建索引
     * @param request
     */
    @RequestMapping("/es/createIndex")
    @ResponseBody
    @ApiOperation(value = "新增索引结构", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "shards", value = "分片数(默认3）", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "replicas", value = "副本数(默认0)", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "mapping", value = "索引结构json", required = true, dataType = "String", paramType = "query"),
    })
    public BaseResult createIndex(HttpServletRequest request)  {
        String index = request.getParameter("index");
        String shards = request.getParameter("shards");
        String replicas = request.getParameter("replicas");
        String mapping = request.getParameter("mapping");

        BaseResult baseResult = new BaseResult();
        CreateIndexRequest createIndex = new CreateIndexRequest(index);
        createIndex.settings(Settings.builder()
                .put("index.number_of_shards", Optional.ofNullable(shards).orElse("3"))
                .put("index.number_of_replicas", Optional.ofNullable(replicas).orElse("0"))
        );
        createIndex.mapping(mapping,
                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(createIndex,RequestOptions.DEFAULT);
            //返回的CreateIndexResponse允许检索有关执行的操作的信息，如下所示：
            boolean acknowledged = createIndexResponse.isAcknowledged();//指示是否所有节点都已确认请求
            baseResult.setObj(JSONArray.toJSON(acknowledged).toString());
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (IOException e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }

//    /**
//     * Template方式搜索，Template已经保存在script目录下
//     * @return
//     */
//    @GetMapping("/es/searchTemplate")
//    public void searchTemplate() throws Exception {
//        Map param = new HashMap();
//        param.put("name", "123");
//        elasticsearchTemplateNew.searchTemplate(param, "tempdemo1", IndexDemo.class).forEach(s -> System.out.println(s));
//    }


    /**
     * 查看索引是否存在
     * @param request
     * @throws IOException
     */
    @ApiOperation(value = "判断索引是否存在", httpMethod = "GET")
    @GetMapping("/es/existsIndex")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
    })
    public BaseResult exists_index(HttpServletRequest request) throws IOException {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        GetIndexRequest getRequest = new GetIndexRequest();
        getRequest.indices(index);
        try {
            boolean exists = client.indices().exists(getRequest, RequestOptions.DEFAULT);
            baseResult.setObj(JSONArray.toJSON(exists).toString());
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (IOException e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }

    /**
     * 删除索引
     */

    @GetMapping("/es/deleteIndex")
    @ResponseBody
    @ApiOperation(value = "删除索引", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
    })
    public BaseResult delete_index(HttpServletRequest request) throws IOException {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest(index);
        try {
            AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteRequest, RequestOptions.DEFAULT);
            baseResult.setObj(JSONArray.toJSON(deleteIndexResponse.isAcknowledged()).toString());
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (IOException e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }

    /**
     * 保存或者更新索引数据
     * @param request
     * @return
     */

    @RequestMapping("/es/commonaddOrUpdate")
    @ApiOperation(value = "新增数据", httpMethod = "POST")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "source", value = "数据json", required = true, dataType = "String", paramType = "query")
    })
    public BaseResult common_add(HttpServletRequest request)  {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String source = request.getParameter("source");
        IndexRequest indexRequest = new IndexRequest(index);
        indexRequest.source(source, XContentType.JSON);
        IndexResponse indexResponse = null;
        try {
            try {
                indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (indexResponse.getResult() == DocWriteResponse.Result.CREATED) {
                baseResult.setObj("创建成功");
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                baseResult.setObj("修改成功");
            }
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }


    /**
     * 根据id更新数据
     * @param request
     */

    @GetMapping("/es/commonUpdateData")
    @ApiOperation(value = "根据id更新数据", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "id", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "source", value = "数据json", required = true, dataType = "String", paramType = "query")
    })
    public BaseResult common_update_data(HttpServletRequest request){
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String id = request.getParameter("id");
        String source = request.getParameter("source");

        try {
            UpdateRequest updateRequest = new UpdateRequest(index, id);

            updateRequest.doc(source);
            UpdateResponse updateResponse = null;
            try {
                updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                baseResult.setObj("创建成功");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                System.out.println("修改成功");
                baseResult.setObj("修改成功");
            }
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }

    /**
     * 删除数据
     * @param request
     */

    @RequestMapping("/es/commonDeleteData")
    @ResponseBody
    @ApiOperation(value = "删除数据", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "id", value = "索引主键", required = true, dataType = "String", paramType = "query")
    })
    public BaseResult common_delete_data(HttpServletRequest request){

        String index = request.getParameter("index");
        String id = request.getParameter("id");
        BaseResult baseResult = new BaseResult();
        DeleteRequest deleteRequest = new DeleteRequest(index , id);

        DeleteResponse deleteResponse = null;
        try {
            deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
            if (deleteResponse.getResult() == DocWriteResponse.Result.DELETED) {
                baseResult.setObj("删除成功");
            }
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (IOException e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }

        return baseResult;

    }


    /**
     * 通过sql查询es数据库
     * @param request
     * @throws Exception
     */
    @ApiOperation(value = "通过sql语句查询es数据库数据", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "sql", value = "sql语句", required = true, dataType = "String", paramType = "query"),
    })
    @GetMapping("/es/commonQueryBySql")
    public BaseResult common_query_sql(HttpServletRequest request) throws Exception {
        String sql = request.getParameter("sql");
        BaseResult baseResult = new BaseResult();
        try {
            String result = elasticsearchTemplateNew.queryBySQL(sql, SqlFormat.JSON);
            SqlResponse sqlResponse = JsonUtils.string2Obj(result, SqlResponse.class);
            List<Map<String,String>> maps = new ArrayList<>();
            sqlResponse.getRows().forEach(row->{
                HashMap<String,String> map = new HashMap<>();
                for(int i=0;i<sqlResponse.getColumns().size();i++){
                    map.put(sqlResponse.getColumns().get(i).getName(),Optional.ofNullable(row.get(i)).orElse(""));
                }
                maps.add(map);
            });
            String json = JsonUtils.obj2String(maps);
            baseResult.setObj(json);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }


    /**
     * 通过sql查询es数据库数量
     * @param request
     * @throws Exception
     */
    @ApiOperation(value = "通过sql语句查询es数据库数量(count)", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "sql", value = "sql语句", required = true, dataType = "String", paramType = "query"),
    })
    @GetMapping("/es/commonQueryCountBySql")
    public BaseResult common_query_count_sql(HttpServletRequest request) throws Exception {
        String sql = request.getParameter("sql");
        BaseResult baseResult = new BaseResult();
        try {
            String result = elasticsearchTemplateNew.queryBySQL(sql, SqlFormat.JSON);
            SqlResponse sqlResponse = JsonUtils.string2Obj(result, SqlResponse.class);
            String json = JsonUtils.obj2String(sqlResponse.getRows().get(0).get(0));
            baseResult.setObj(json);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }



//    /**
//     * 通过sql分页查询es数据库
//     * @param request
//     * @throws Exception
//     */
//
//
//    @GetMapping("/es/commonQueryBySqlPage")
//    public void common_query_sqlPage(HttpServletRequest request) throws Exception {
//        String sql = request.getParameter("sql");
//        String count = request.getParameter("count");
//        String result = elasticsearchTemplateNew.queryBySQL(sql, SqlFormat.JSON);
//        String resultCount = elasticsearchTemplateNew.queryBySQL(count,SqlFormat.JSON);
//        SqlResponse sqlResponse = JsonUtils.string2Obj(result, SqlResponse.class);
//        List<Map<String,String>> maps = new ArrayList<>();
//        sqlResponse.getRows().forEach(row->{
//            HashMap<String,String> map = new HashMap<>();
//            for(int i=0;i<sqlResponse.getColumns().size();i++){
//                map.put(sqlResponse.getColumns().get(i).getName(),Optional.ofNullable(row.get(i)).orElse(""));
//            }
//            maps.add(map);
//        });
//        String json = JsonUtils.obj2String(maps);
//    }



    @ApiOperation(value = "通用分页查询", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "index索引名称", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "查询参数([{\"column\":\"incSource.keyword\",\"operator\":\"and\",\"type\":\"equals\",\"val\":\"我要报\"},{\"column\":\"issentry\",\"operator\":\"and\",\"type\":\"equals\",\"val\":\"1\"},{\"operator\":\"or\", \"ors\": [{\"column\":\"incId\",\"type\":\"equals\",\"val\":\"020c1ee0167b11ec6b9a6df783dfa9cc\"}, {\"column\":\"incId\",\"type\":\"equals\",\"val\":\"032d99f02d7011ec8f0b9b0fd79730b2\"}]}])", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "curPage", value = "当前页(默认0）", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "pageSize", value = "每页大小(默认10）", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "sortFields", value = "排序字段", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "highLightField", value = "高亮字段", required = false, dataType = "String", paramType = "query")
    })
    @GetMapping("/es/commonQueryByPage")
    public BaseResult common_query_page(HttpServletRequest request){
        String index = request.getParameter("index");
        String searchParamJson = request.getParameter("searchParam");
        String curPage = Optional.ofNullable(request.getParameter("curPage")).orElse("0");
        String pageSize = Optional.ofNullable(request.getParameter("pageSize")).orElse("10");
        String sortFields = request.getParameter("sortFields");
        String highLightField = request.getParameter("highLightField");
        BaseResult baseResult = new BaseResult();
        try {
            ESQueryHelper esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);

            if(highLightField!=null){
                esQueryHelper.highlighter(highLightField,null,null);
            }

            if(sortFields!=null){
                List<SortField> sortFieldList = JSON.parseArray(sortFields, SortField.class);
                sortFieldList.forEach(sortField -> {
                    if("asc".equals(sortField.getSort())){
                        esQueryHelper.asc(sortField.getField());
                    }else {
                        esQueryHelper.desc(sortField.getField());
                    }
                });
            }


            esQueryHelper.from(Integer.valueOf(curPage));
            esQueryHelper.size(Integer.valueOf(pageSize));

            SearchResponse searchResponse = esQueryHelper.execute(client);// 异常自己处理


            List<Map<String,Object>> list = new ArrayList<>();


            for(SearchHit hit:searchResponse.getHits()){
                //获取高亮字段
                Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                Map<String,Object> map = JSON.parseObject(hit.getSourceAsString());
                for(Map.Entry<String,HighlightField> entry:highlightFields.entrySet()){
                    String key = entry.getKey().replace(".keyword","");
                    if(map.containsKey(key)){
                        map.put(key, Joiner.on("").join(entry.getValue().fragments()));
                    }
                }
                list.add(map);

            }


            Long value = searchResponse.getHits().getTotalHits().value;
            PageList<Map<String,Object>> pageList = new PageList<>();
            pageList.setList(list);
            pageList.setCurrentPage(Integer.parseInt(curPage));
            pageList.setPageSize(Integer.parseInt(pageSize));
            pageList.setTotalElements(value);
            baseResult.setObj(pageList);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (NumberFormatException | IOException e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }



    @ApiOperation(value = "通用查询", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "index索引名称", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "查询参数", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "curPage", value = "当前页(默认0）", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "pageSize", value = "每页大小(默认10）", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "sortFields", value = "排序字段", required = false, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonQuery")
    public BaseResult common_query(HttpServletRequest request){
        String index = request.getParameter("index");
        String searchParamJson = request.getParameter("searchParam");
        String sortFields = request.getParameter("sortFields");
        BaseResult baseResult = new BaseResult();
        try {
            ESQueryHelper esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);
            if(sortFields!=null){
                List<SortField> sortFieldList = JSON.parseArray(sortFields, SortField.class);
                sortFieldList.forEach(sortField -> {
                    if("asc".equals(sortField.getSort())){
                        esQueryHelper.asc(sortField.getField());
                    }else {
                        esQueryHelper.desc(sortField.getField());
                    }
                });
            }
            SearchResponse searchResponse = esQueryHelper.execute(client);// 异常自己处理
            List<String> list = Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getSourceAsString).collect(Collectors.toList());
            Long value = searchResponse.getHits().getTotalHits().value;
            PageList<String> pageList = new PageList<>();
            pageList.setList(list);
            pageList.setTotalElements(value);
            baseResult.setObj(pageList);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (NumberFormatException | IOException e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }


    @ApiOperation(value = "通用查询数量", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "index索引名称", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "查询参数", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonQueryCount")
    public BaseResult common_query_count(HttpServletRequest request){
        String index = request.getParameter("index");
        String searchParamJson = request.getParameter("searchParam");
        BaseResult baseResult = new BaseResult();
        try {
            ESQueryHelper esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);
            CountRequest countRequest = new CountRequest(index);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(esQueryHelper.getBool());
            countRequest.source(searchSourceBuilder);
            CountResponse countResponse = client.count(countRequest, RequestOptions.DEFAULT);
            long count = countResponse.getCount();
            baseResult.setObj(count);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (NumberFormatException | IOException e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }


    /**
     * 为索引增加别名
     * @param request
     * @return
     * @throws Exception
     */

    @ApiOperation(value = "为索引增加别名", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "indexName", value = "需要修改的索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "aliasName", value = "需要增加的别名", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonAddAlias")
    public BaseResult addAlias(HttpServletRequest request) throws Exception {
        String indexName = request.getParameter("indexName");
        String aliasName = request.getParameter("aliasName");
        BaseResult baseResult = new BaseResult();
        try {
            Boolean isAcknowledged = elasticsearchTemplateNew.addAlias(indexName,aliasName);
            baseResult.setObj(isAcknowledged);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }



    /**
     * 为索引增加别名
     * @param request
     * @return
     * @throws Exception
     */

    @ApiOperation(value = "为索引删除别名", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "indexName", value = "需要修改的索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "aliasName", value = "需要删除的别名", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonDropAlias")
    public BaseResult dropAlias(HttpServletRequest request) throws Exception {
        String indexName = request.getParameter("indexName");
        String aliasName = request.getParameter("aliasName");
        BaseResult baseResult = new BaseResult();
        try {
            Boolean isAcknowledged = elasticsearchTemplateNew.dropAlias(indexName,aliasName);
            baseResult.setObj(isAcknowledged);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }



    @ApiOperation(value = "重建索引，拷贝数据", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "oldIndexname", value = "需要重建的老索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "newIndexname", value = "需要重建的新索引名", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonReindex")
    public BaseResult reindex(HttpServletRequest request) throws Exception {
        String oldIndexname = request.getParameter("oldIndexname");
        String newIndexname = request.getParameter("newIndexname");
        BaseResult baseResult = new BaseResult();
        try {
            elasticsearchTemplateNew.reindex(oldIndexname,newIndexname);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }




    @ApiOperation(value = "Metric Aggregation ES指标聚合\n" +
            "     avg 平均值\n" +
            "     max 最大值\n" +
            "     min 最小值\n" +
            "     sum 和\n" +
            "     value_count 数量\n" +
            "     cardinality 基数（distinct去重）", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "查询参数", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "field", value = "字段名", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonAggMetric")
    public BaseResult aggMetric(HttpServletRequest request) throws Exception {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String field = request.getParameter("field");
        String searchParamJson = request.getParameter("searchParam");

        try {
            ESQueryHelper esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);
            Map<String,Object> result = elasticsearchTemplateNew.aggMetric(index,field,esQueryHelper);
            baseResult.setObj(result);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }


    @ApiOperation(value = "使用field字段进行桶分组聚合", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "索引名", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "查询参数", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "field", value = "字段名", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonAggTerms")
    public BaseResult aggTerms(HttpServletRequest request) throws Exception {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String field = request.getParameter("field");
        String searchParamJson = request.getParameter("searchParam");
        try {
            ESQueryHelper esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);
            Map<String,Long> result = elasticsearchTemplateNew.aggTerms(index,field,esQueryHelper);
            baseResult.setObj(result);
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }

}
