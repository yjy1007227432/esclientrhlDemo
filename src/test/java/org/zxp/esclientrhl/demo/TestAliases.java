package org.zxp.esclientrhl.demo;

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.AliasDemo;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;

import java.util.List;

/**
 * @program: 别名的使用
 * @description:
 * 索引必须提前创建，别名自动创建
 * @author: X-Pacific zhang
 * @create: 2021-04-07 14:21
 **/
public class TestAliases extends EsclientrhlDemoApplicationTests  {
    @Autowired
    ElasticsearchTemplate<AliasDemo,String> elasticsearchTemplate2;

    @Test
    public void testAlias() throws Exception {
        elasticsearchTemplate2.save(new AliasDemo("zxp001"));
        List<AliasDemo> search = elasticsearchTemplate2.search(QueryBuilders.matchAllQuery(), AliasDemo.class);
        search.forEach(System.out::println);
    }
}
