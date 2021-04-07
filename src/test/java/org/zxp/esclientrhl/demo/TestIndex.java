package org.zxp.esclientrhl.demo;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.index.ElasticsearchIndex;

/**
 * @program: 索引管理
 * @description: ${description}
 * @author: X-Pacific zhang
 * @create: 2019-02-25 14:13
 **/
public class TestIndex extends EsclientrhlDemoApplicationTests{
    @Autowired
    ElasticsearchIndex<IndexDemo> elasticsearchIndex;

    @Test
    public void testIndex() throws Exception {
        if(!elasticsearchIndex.exists(IndexDemo.class)){
            elasticsearchIndex.dropIndex(IndexDemo.class);
            elasticsearchIndex.createIndex(IndexDemo.class);
        }
    }
}
