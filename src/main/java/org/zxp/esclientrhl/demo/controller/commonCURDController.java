package org.zxp.esclientrhl.demo.controller;


import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.demo.domain.MyMovies;
import org.zxp.esclientrhl.demo.response.SqlResponse;
import org.zxp.esclientrhl.demo.service.ElasticsearchTemplateNew;
import org.zxp.esclientrhl.enums.SqlFormat;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;
import org.zxp.esclientrhl.util.JsonUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.*;

@RestController
@Api(tags = "es常用操作")
@Slf4j
@Validated
public class commonCURDController {


    @Autowired
    private RestHighLevelClient client;

    @Qualifier(value = "ElasticsearchTemplateNew")
    private ElasticsearchTemplateNew elasticsearchTemplateNew;


    /**
     * 创建索引
     * @param request
     */

    @GetMapping("/es/createIndex")
    @ApiOperation("创建索引")
    public void createIndex(HttpServletRequest request)  {
        String index = request.getParameter("index");
        String shards = request.getParameter("shards");
        String replicas = request.getParameter("replicas");
        String mapping = request.getParameter("mapping");
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
            System.out.println(acknowledged);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @GetMapping("/es/searchTemplate")
    public void searchTemplate() throws Exception {
        Map param = new HashMap();
        param.put("name", "123");
        elasticsearchTemplateNew.searchTemplate(param, "tempdemo1", IndexDemo.class).forEach(s -> System.out.println(s));
    }


    /**
     * 查看索引是否存在
     * @param request
     * @throws IOException
     */
    @GetMapping("/es/existsIndex")
    public void exists_index(HttpServletRequest request) throws IOException {
        String index = request.getParameter("index");
        GetIndexRequest getRequest = new GetIndexRequest();
        getRequest.indices(index);
        boolean exists = client.indices().exists(getRequest, RequestOptions.DEFAULT);
        System.out.println(exists);
    }

    /**
     * 删除索引
     */

    @GetMapping("/es/deleteIndex")
    public void delete_index(HttpServletRequest request) throws IOException {
        String index = request.getParameter("index");
        DeleteIndexRequest deleteRequest = new DeleteIndexRequest("esdemo");
        AcknowledgedResponse deleteIndexResponse = client.indices().delete(deleteRequest, RequestOptions.DEFAULT);
        System.out.println(deleteIndexResponse.isAcknowledged());
    }

    /**
     * 保存或者更新索引数据
     * @param request
     * @return
     */

    @GetMapping("/es/commonaddOrUpdate")
    public String common_add(HttpServletRequest request)  {
        String index = request.getParameter("index");
        String source = request.getParameter("source");
        IndexRequest indexRequest = new IndexRequest(index);
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
        return null;
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

    @GetMapping("/es/commonDeleteData")
    public void common_delete_data(HttpServletRequest request){

        String index = request.getParameter("index");
        String id = request.getParameter("id");

        DeleteRequest deleteRequest = new DeleteRequest(index , id);

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
     * 通过sql查询es数据库
     * @param request
     * @throws Exception
     */

    @GetMapping("/es/commonQueryBySql")
    public void common_query_sql(HttpServletRequest request) throws Exception {
        String sql = request.getParameter("sql");
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

}
