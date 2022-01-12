package org.zxp.esclientrhl.demo.controller;


import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zxp.esclientrhl.demo.domain.IndexDemo;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Date;
import java.util.Optional;

@RestController
public class commonCURDController {


    @Autowired
    RestHighLevelClient client;



    @GetMapping("/es/commonadd")
    public String commonAdd(HttpServletRequest request)  {
        String index = request.getParameter("index");
        String source = request.getParameter("source");
        IndexRequest indexRequest = new IndexRequest(index);
        IndexDemo m1 = new IndexDemo();
        m1.setProposal_no("111222333aaabbbccc_01");
        m1.setRisk_code("0101");
        m1.setOperate_date(new Date());

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


    @GetMapping("/es/createIndex")
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
}
