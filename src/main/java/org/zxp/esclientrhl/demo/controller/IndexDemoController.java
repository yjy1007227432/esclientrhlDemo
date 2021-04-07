package org.zxp.esclientrhl.demo.controller;

import org.elasticsearch.index.query.QueryBuilders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.demo.repository.IndexDemoRepository;

import java.util.Arrays;
import java.util.List;

/**
 * @program: esclientrhlDemo
 * @description:
 * @author: X-Pacific zhang
 * @create: 2021-03-10 15:56
 **/
@RestController
public class IndexDemoController {
    @Autowired
    private IndexDemoRepository indexDemoRepository;
    //http://127.0.0.1:8888/demo/add
    @GetMapping("/demo/add")
    public String add() throws Exception {
        IndexDemo indexDemo = new IndexDemo();
        indexDemo.setProposal_no("1");
        indexDemo.setAppli_name("a1");
        indexDemo.setRisk_code("aa1");
        indexDemo.setSum_premium(1);
        indexDemoRepository.save(indexDemo);
        return "新增成功";
    }
    //http://127.0.0.1:8888/demo/add_list
    @GetMapping("/demo/add_list")
    public String addList() throws Exception {
        IndexDemo indexDemo2 = new IndexDemo();
        indexDemo2.setProposal_no("2");
        indexDemo2.setAppli_name("a2");
        indexDemo2.setRisk_code("aa2");
        indexDemo2.setSum_premium(2);

        IndexDemo indexDemo3 = new IndexDemo();
        indexDemo3.setProposal_no("3");
        indexDemo3.setAppli_name("a3");
        indexDemo3.setRisk_code("aa3");
        indexDemo3.setSum_premium(3);
        indexDemoRepository.save(Arrays.asList(indexDemo2,indexDemo3));
        return "新增成功";
    }
    //http://127.0.0.1:8888/demo/update
    @GetMapping("/demo/update")
    public String update() throws Exception {
        IndexDemo indexDemo = new IndexDemo();
        indexDemo.setProposal_no("1");
        indexDemo.setAppli_name("a999999");
        indexDemo.setRisk_code("aa9999999");
        indexDemo.setSum_premium(99999);
        indexDemoRepository.update(indexDemo);
        return "修改成功";
    }

    //http://127.0.0.1:8888/demo/delete
    @GetMapping("/demo/delete")
    public String delete() throws Exception {
        IndexDemo indexDemo = new IndexDemo();
        indexDemo.setProposal_no("1");
        indexDemo.setAppli_name("a999999");
        indexDemo.setRisk_code("a9999999");
        indexDemo.setSum_premium(99999);
        indexDemoRepository.delete(indexDemo);
        return "删除成功";
    }
    //http://127.0.0.1:8888/demo/query
    @GetMapping("/demo/query")
    public List<IndexDemo> query() throws Exception {
        List<IndexDemo> search = indexDemoRepository.search(QueryBuilders.matchAllQuery());
        return search;
    }


    @GetMapping("/demo/query2")
    public List<IndexDemo> query2() throws Exception {

        List<IndexDemo> search = indexDemoRepository.search(QueryBuilders.matchAllQuery());
        return search;
    }
}
