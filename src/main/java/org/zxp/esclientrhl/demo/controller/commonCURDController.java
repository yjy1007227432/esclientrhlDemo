package org.zxp.esclientrhl.demo.controller;


import org.apache.http.HttpRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.util.JsonUtils;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Date;

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
}
