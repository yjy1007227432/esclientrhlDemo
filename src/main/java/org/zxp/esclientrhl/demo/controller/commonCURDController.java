package org.zxp.esclientrhl.demo.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@Api(tags = "es????????????")
@Slf4j
@Validated
public class commonCURDController {
    @Autowired
    private RestHighLevelClient client;

    @Autowired
    private ElasticsearchTemplateNew elasticsearchTemplateNew;


    /**
     * ????????????
     * @param request
     */
    @RequestMapping("/es/createIndex")
    @ResponseBody
    @ApiOperation(value = "??????????????????", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "shards", value = "?????????(??????3???", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "replicas", value = "?????????(??????0)", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "mapping", value = "????????????json", required = true, dataType = "String", paramType = "query"),
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
        createIndex.mapping("_doc",mapping,
                XContentType.JSON);
        try {
            CreateIndexResponse createIndexResponse = client.indices().create(createIndex,RequestOptions.DEFAULT);
            //?????????CreateIndexResponse????????????????????????????????????????????????????????????
            boolean acknowledged = createIndexResponse.isAcknowledged();//??????????????????????????????????????????
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
//     * Template???????????????Template???????????????script?????????
//     * @return
//     */
//    @GetMapping("/es/searchTemplate")
//    public void searchTemplate() throws Exception {
//        Map param = new HashMap();
//        param.put("name", "123");
//        elasticsearchTemplateNew.searchTemplate(param, "tempdemo1", IndexDemo.class).forEach(s -> System.out.println(s));
//    }


    /**
     * ????????????????????????
     * @param request
     * @throws IOException
     */
    @ApiOperation(value = "????????????????????????", httpMethod = "GET")
    @GetMapping("/es/existsIndex")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
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
     * ????????????
     */

    @GetMapping("/es/deleteIndex")
    @ResponseBody
    @ApiOperation(value = "????????????", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
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
     * ??????????????????????????????
     * @param request
     * @return
     */

    @RequestMapping("/es/commonaddOrUpdate")
    @ApiOperation(value = "????????????", httpMethod = "POST")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "source", value = "??????json", required = true, dataType = "String", paramType = "query")
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
                baseResult.setObj("????????????");
            } else if (indexResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                baseResult.setObj("????????????");
            }
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }




    @RequestMapping("/es/commonBulkAddOrUpdate")
    @ApiOperation(value = "??????????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "source", value = "??????json", required = true, dataType = "String", paramType = "query")
    })
    public BaseResult common_bulk_add(HttpServletRequest request)  {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String source = request.getParameter("source");
        List<Object> objects = JSON.parseArray(source,Object.class);
        BulkRequest bulkRequest = new BulkRequest(index);
        objects.forEach(o -> {
            IndexRequest indexRequest = new IndexRequest(index);
            indexRequest.id(JSONArray.parseObject(o.toString()).getString("id"));
            indexRequest.source(JSON.toJSONString(o), XContentType.JSON);
            bulkRequest.add(indexRequest);
        });
        BulkResponse bulkResponse = null;
        try {
            bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
            if (!bulkResponse.hasFailures()) {
                baseResult.setObj("????????????");
                baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
                baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
            }else {
                baseResult.setResultCode(Constants.RESULTCODE_FAIL);
                baseResult.setResultMsg(Constants.OPERATION_FAIL + ":" + bulkResponse.buildFailureMessage());
            }
        } catch (Exception e) {
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }


    /**
     * ??????id????????????
     * @param request
     */

    @GetMapping("/es/commonUpdateData")
    @ApiOperation(value = "??????id????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "id", value = "id", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "source", value = "??????json", required = true, dataType = "String", paramType = "query")
    })
    public BaseResult common_update_data(HttpServletRequest request){
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String id = request.getParameter("id");
        String source = request.getParameter("source");

        Map<String,Object> map = JSON.parseObject(source, HashMap.class);
        try {
            UpdateRequest updateRequest = new UpdateRequest(index, id);

            updateRequest.doc(map);
            UpdateResponse updateResponse = null;
            try {
                updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                e.printStackTrace();
            }
            if (updateResponse.getResult() == DocWriteResponse.Result.CREATED) {
                baseResult.setObj("????????????");
            } else if (updateResponse.getResult() == DocWriteResponse.Result.UPDATED) {
                baseResult.setObj("????????????");
            }
            baseResult.setResultCode(Constants.RESULTCODE_SUCCESS);
            baseResult.setResultMsg(Constants.OPERATION_SUCCESS);
        } catch (Exception e) {
            System.out.println(e.getMessage());
            baseResult.setResultCode(Constants.RESULTCODE_FAIL);
            baseResult.setResultMsg(Constants.OPERATION_FAIL+":"+e);
        }
        return baseResult;
    }

    /**
     * ????????????
     * @param request
     */

    @RequestMapping("/es/commonDeleteData")
    @ResponseBody
    @ApiOperation(value = "????????????", httpMethod = "GET")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "id", value = "????????????", required = true, dataType = "String", paramType = "query")
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
                baseResult.setObj("????????????");
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
     * ??????sql??????es?????????
     * @param request
     * @throws Exception
     */
    @ApiOperation(value = "??????sql????????????es???????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "sql", value = "sql??????", required = true, dataType = "String", paramType = "query"),
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
     * ??????sql??????es???????????????
     * @param request
     * @throws Exception
     */
    @ApiOperation(value = "??????sql????????????es???????????????(count)", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "sql", value = "sql??????", required = true, dataType = "String", paramType = "query"),
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
//     * ??????sql????????????es?????????
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



    @ApiOperation(value = "??????????????????", httpMethod = "GET", produces = "application/json; charset=utf-8")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "index????????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "????????????([{\"column\":\"incSource.keyword\",\"operator\":\"and\",\"type\":\"equals\",\"val\":\"?????????\"},{\"column\":\"issentry\",\"operator\":\"and\",\"type\":\"equals\",\"val\":\"1\"},{\"operator\":\"or\", \"ors\": [{\"column\":\"incId\",\"type\":\"equals\",\"val\":\"020c1ee0167b11ec6b9a6df783dfa9cc\"}, {\"column\":\"incId\",\"type\":\"equals\",\"val\":\"032d99f02d7011ec8f0b9b0fd79730b2\"}]}])", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "curPage", value = "?????????(??????0???", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "pageSize", value = "????????????(??????10???", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "sortFields", value = "????????????", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "highLightField", value = "????????????", required = false, dataType = "String", paramType = "query")
    })
    @RequestMapping(value = "/es/commonQueryByPage", produces = "application/json; charset=utf-8")
    public BaseResult common_query_page(HttpServletRequest request, HttpServletResponse response){
        String index = request.getParameter("index");
        String searchParamJson = request.getParameter("searchParam");
        String curPage = Optional.ofNullable(request.getParameter("curPage")).orElse("0");
        String pageSize = Optional.ofNullable(request.getParameter("pageSize")).orElse("10");
        String sortFields = request.getParameter("sortFields");
        String highLightField = request.getParameter("highLightField");
        BaseResult baseResult = new BaseResult();
        try {
            ESQueryHelper esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);

            if(highLightField!=null&&!highLightField.equals("null")){
                esQueryHelper.highlighter(highLightField,null,null);
            }


            if(sortFields!=null&&!sortFields.equals("null")&&!IsContainMatch(searchParamJson)){
                List<SortField> sortFieldList = JSON.parseArray(sortFields, SortField.class);
                sortFieldList.forEach(sortField -> {
                    if("asc".equals(sortField.getSort())){
                        esQueryHelper.asc(sortField.getField());
                    }else {
                        esQueryHelper.desc(sortField.getField());
                    }
                });
            }


            esQueryHelper.from((Integer.parseInt(curPage)-1)*Integer.parseInt(pageSize));
            esQueryHelper.size(Integer.valueOf(pageSize));

            SearchResponse searchResponse = esQueryHelper.execute(client);// ??????????????????


            List<Map<String,Object>> list = new ArrayList<>();


            for(SearchHit hit:searchResponse.getHits()){
                //??????????????????
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



    @ApiOperation(value = "????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "index????????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "????????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "curPage", value = "?????????(??????0???", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "pageSize", value = "????????????(??????10???", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "sortFields", value = "????????????", required = false, dataType = "String", paramType = "query")})
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
            SearchResponse searchResponse = esQueryHelper.execute(client);// ??????????????????
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


    @ApiOperation(value = "??????????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "index????????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "????????????", required = true, dataType = "String", paramType = "query")})
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
     * ?????????????????????
     * @param request
     * @return
     * @throws Exception
     */

    @ApiOperation(value = "?????????????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "indexName", value = "????????????????????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "aliasName", value = "?????????????????????", required = true, dataType = "String", paramType = "query")})
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
     * ?????????????????????
     * @param request
     * @return
     * @throws Exception
     */

    @ApiOperation(value = "?????????????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "indexName", value = "????????????????????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "aliasName", value = "?????????????????????", required = true, dataType = "String", paramType = "query")})
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



    @ApiOperation(value = "???????????????????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "oldIndexname", value = "???????????????????????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "newIndexname", value = "???????????????????????????", required = true, dataType = "String", paramType = "query")})
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




    @ApiOperation(value = "Metric Aggregation ES????????????\n" +
            "     avg ?????????\n" +
            "     max ?????????\n" +
            "     min ?????????\n" +
            "     sum ???\n" +
            "     value_count ??????\n" +
            "     cardinality ?????????distinct?????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "????????????", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "field", value = "?????????", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonAggMetric")
    public BaseResult aggMetric(HttpServletRequest request) throws Exception {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String field = request.getParameter("field");
        String searchParamJson = request.getParameter("searchParam");
        ESQueryHelper esQueryHelper;
        try {
            if(searchParamJson!=null){
                esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);
            }else {
                esQueryHelper = ESQueryHelper.build(index);
            }
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


    @ApiOperation(value = "??????field???????????????????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "?????????", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "????????????", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "field", value = "?????????", required = true, dataType = "String", paramType = "query")})
    @GetMapping("/es/commonAggTerms")
    public BaseResult aggTerms(HttpServletRequest request) throws Exception {
        BaseResult baseResult = new BaseResult();
        String index = request.getParameter("index");
        String field = request.getParameter("field");
        String searchParamJson = request.getParameter("searchParam");
        try {
            ESQueryHelper esQueryHelper;
            if(searchParamJson!=null){
                esQueryHelper = ParseSearchParamUtil.ParseSearchParamUtil(searchParamJson,index);
            }else {
                esQueryHelper = ESQueryHelper.build(index);
            }
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


    public boolean IsContainMatch(String searchParamJson){
        List<SearchParam> searchParams = JSON.parseArray(searchParamJson, SearchParam.class);
        for(SearchParam searchParam:searchParams){
            if("match".equals(searchParam.getType())){
                return true;
            }
            if("or".equals(searchParam.getOperator())){
                List<SearchParam.Or> ors = JSON.parseArray(JSONObject.toJSONString(searchParam.getOrs()), SearchParam.Or.class);
                for(SearchParam.Or or:ors){
                    if("match".equals(or.getType())){
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * ??????sql??????es?????????
     * @param request
     * @throws Exception
     */
    @ApiOperation(value = "??????sql??????????????????es???????????????", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "sql", value = "sql??????", required = true, dataType = "String", paramType = "query"),
    })
    @GetMapping("/es/commonQueryBySqlPage")
    public BaseResult common_query_sql_page(HttpServletRequest request) throws Exception {
        String sql = request.getParameter("sql");
        BaseResult baseResult = new BaseResult();
        try {
            String result = elasticsearchTemplateNew.queryBySQL(sql, SqlFormat.JSON);

            JSONArray array = JSONArray.parseArray(result);
            List  list = getSubList(array,1,2);






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
     * ??????????????????
     * @param rows ???????????????????????????json????????????
     * @param pageNum ????????????
     * @param pageSize ????????????
     * @return
     */
    private List getSubList(JSONArray rows, Integer pageNum, Integer pageSize) {
        //????????????
        int startIndex = 0;
        int endIndex = rows.size();
        if (pageNum != null && pageSize != null) {
            startIndex = getStartIndex(pageNum, pageSize);
            endIndex = getEndIndex(pageNum, pageSize);
        }
        List subList;
        if (rows.size() >= endIndex) {
            subList = rows.subList(startIndex, endIndex);
        } else {
            subList = rows.subList(startIndex, rows.size());
        }
        return subList;
    }
    /**
     * ??????????????????
     * @return
     */
    public static int getStartIndex(int pageNum,int pageSize){
        return 0+(pageNum-1)*pageSize;
    }
    /**
     * ??????????????????
     * @return
     */
    public static int getEndIndex(int pageNum,int pageSize){
        return pageNum*pageSize;
    }


}
