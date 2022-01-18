package org.zxp.esclientrhl.demo.controller;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
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
    private static Log log = LogFactory.getLog(commonCURDController.class);

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
    public void common_update_data(HttpServletRequest request){
        String index = request.getParameter("index");
        String id = request.getParameter("id");
        String source = request.getParameter("source");

        UpdateRequest updateRequest = new UpdateRequest(index, id);

        updateRequest.doc(source);
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
    @ApiOperation(value = "通过sql语句查询es数据库", httpMethod = "GET")
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
     * 通过sql分页查询es数据库
     * @param request
     * @throws Exception
     */


    @GetMapping("/es/commonQueryBySqlPage")
    public void common_query_sqlPage(HttpServletRequest request) throws Exception {
        String sql = request.getParameter("sql");
        String count = request.getParameter("count");
        String result = elasticsearchTemplateNew.queryBySQL(sql, SqlFormat.JSON);
        String resultCount = elasticsearchTemplateNew.queryBySQL(count,SqlFormat.JSON);
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
    }



    @ApiOperation(value = "通用查询", httpMethod = "GET")
    @ResponseBody
    @ApiImplicitParams({
            @ApiImplicitParam(name = "index", value = "index索引名称", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "searchParam", value = "查询参数", required = true, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "curPage", value = "当前页(默认0）", required = false, dataType = "String", paramType = "query"),
            @ApiImplicitParam(name = "pageSize", value = "每页大小(默认10）", required = false, dataType = "String", paramType = "query"),
    })
    @GetMapping("/es/commonQueryByPage")
    public BaseResult common_query(HttpServletRequest request) throws IOException {
        String index = request.getParameter("index");
        String searchParamJson = request.getParameter("searchParam");
        String curPage = Optional.ofNullable(request.getParameter("curPage")).orElse("0");
        String pageSize = Optional.ofNullable(request.getParameter("pageSize")).orElse("10");
        BaseResult baseResult = new BaseResult();
        try {
            List<SearchParam> searchParams = JSON.parseArray(searchParamJson, SearchParam.class);
            ESQueryHelper esQueryHelper = ESQueryHelper.build(index);

            searchParams.forEach(searchParam -> {
                if("and".equals(searchParam.getOperator())) {
                    switch (searchParam.getType()) {
                        case "equals":esQueryHelper.and(ESQueryHelper.equals(searchParam.getColumn(),searchParam.getVal()));break;
                        case "in":esQueryHelper.and(ESQueryHelper.in(searchParam.getColumn(),searchParam.getVal()));break;
                        case "like":esQueryHelper.and(ESQueryHelper.like(searchParam.getColumn(),searchParam.getVal().toString()));break;
                        case "leftLike":esQueryHelper.and(ESQueryHelper.leftLike(searchParam.getColumn(),searchParam.getVal().toString()));break;
                        case "rightLike":esQueryHelper.and(ESQueryHelper.rightLike(searchParam.getColumn(),searchParam.getVal().toString()));break;
                        case "gt":esQueryHelper.and(ESQueryHelper.gt(searchParam.getColumn(),searchParam.getVal().toString()));break;
                        case "lt":esQueryHelper.and(ESQueryHelper.lt(searchParam.getColumn(),searchParam.getVal().toString()));break;
                        case "between": esQueryHelper.and(QueryBuilders.rangeQuery(searchParam.getColumn()).gt(searchParam.getVal().toString().split(",")[0]).lte(searchParam.getVal().toString().split(",")[1]));break;
                        case "match":esQueryHelper.and(ESQueryHelper.match(searchParam.getColumn(),searchParam.getVal()));break;
                    }
                }else {
                    esQueryHelper.orNew(searchParam.getOrs());
                }
            });

            esQueryHelper.from(Integer.valueOf(curPage));
            esQueryHelper.size(Integer.valueOf(pageSize));

            SearchResponse searchResponse = esQueryHelper.execute(client);// 异常自己处理
            List<String> list = Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getSourceAsString).collect(Collectors.toList());
            Long value = searchResponse.getHits().getTotalHits().value;
            PageList<String> pageList = new PageList<>();
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

}
