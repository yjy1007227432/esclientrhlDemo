package org.zxp.esclientrhl.demo;

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.RolloverDemo;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;

import java.util.Arrays;

/**
 * @program: Rollover的使用
 * @description:
 * @author: X-Pacific zhang
 * @create: 2021-04-07 15:01
 **/
public class TestRollover extends EsclientrhlDemoApplicationTests {
    @Autowired
    ElasticsearchTemplate<RolloverDemo,String> elasticsearchTemplate;
    @Test
    public void testRollover() throws Exception {
        elasticsearchTemplate.save(Arrays.asList(new RolloverDemo("a"),new RolloverDemo("b"),new RolloverDemo("c")));

        Thread.sleep(2000);
        elasticsearchTemplate.search(QueryBuilders.matchAllQuery(), RolloverDemo.class).forEach(s -> System.out.println(s));
        elasticsearchTemplate.search(QueryBuilders.matchAllQuery(), RolloverDemo.class).forEach(s -> System.out.println(s));
        elasticsearchTemplate.delete(new RolloverDemo("1","a"));
        elasticsearchTemplate.save(new RolloverDemo("4","d"));
        elasticsearchTemplate.update(new RolloverDemo("4","4d"));
        elasticsearchTemplate.search(QueryBuilders.matchAllQuery(), RolloverDemo.class).forEach(s -> System.out.println(s));
    }
}
