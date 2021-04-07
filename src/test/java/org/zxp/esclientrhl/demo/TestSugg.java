package org.zxp.esclientrhl.demo;

import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.Sugg;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;
import org.zxp.esclientrhl.repository.ElasticsearchTemplateImpl;

/**
 * @program: 搜索建议的使用
 * @description:
 * 几种suggester对比
 * Complate和其他suggester区别
 * Complate只能前缀搜索，性能好，适合频繁的搜索提示功能,FST
 * 其他suggester 不是必须前缀搜索，功能更全面，适合不频繁但搜索更加复杂，需要建议的更到位的功能
 * ngram与Complate 区别
 * ngram需要自行定制MAPPING
 * ngram用来解决前缀搜索扫描整个倒排索引的问题，通过match phrace性能还不错
 * ngram不像Complate 需要特殊的数据结构FST，而是倒排索引，并且不存内存
 * ngram可以从非开头的词的前缀搜（每个词的前缀，completion多长的句子也只能前缀搜）
 * ngram普通搜索就可以，不需要用到suggester
 *
 * 什么是edge ngram
 * 是在已经分词的基础上进行拆分重新分词
 * "abc cba" -> a ab abc c cb cba
 * 可以用来解决前缀搜索扫描整个倒排索引的问题
 * @author: X-Pacific zhang
 * @create: 2019-10-11 12:57
 **/
public class TestSugg extends EsclientrhlDemoApplicationTests {
    @Autowired
    ElasticsearchTemplate<Sugg,String> elasticsearchTemplate;

    /**
     * ComplateSuggester
     * @throws Exception
     */
    @Test
    public void testComplateSuggester() throws Exception {
        System.out.println("#######");
        elasticsearchTemplate.completionSuggest
                ("appno", "1234", Sugg.class).forEach(s -> System.out.println(s));
    }

    /**
     * Ngram
     * @throws Exception
     */
    @Test
    public void testNgram() throws Exception {
        System.out.println("#######");
        elasticsearchTemplate.search(QueryBuilders.matchPhraseQuery("msg","che "),Sugg.class).forEach(s -> System.out.println(s));
    }

    /**
     * PhraseSuggester
     * @throws Exception
     */
    @Test
    public void testPhraseSuggester() throws Exception {
        System.out.println("#######");
        ElasticsearchTemplateImpl.PhraseSuggestParam param
                = new ElasticsearchTemplateImpl.PhraseSuggestParam(5,1,null,"always");
        elasticsearchTemplate.phraseSuggest("body", "who is good boy zhangxinpen may be a goop",param, Sugg.class).forEach(s -> System.out.println(s));
    }

}
