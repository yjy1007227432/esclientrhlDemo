package org.zxp.esclientrhl.demo;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.Actors;
import org.zxp.esclientrhl.demo.domain.MyMovies;
import org.zxp.esclientrhl.enums.AggsType;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;
import org.zxp.esclientrhl.repository.ElasticsearchTemplateImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @program: 嵌套对象的使用
 * @description:
 * @author: X-Pacific zhang
 * @create: 2020-01-01 11:26
 **/
public class TestNested extends EsclientrhlDemoApplicationTests  {
    @Autowired
    ElasticsearchTemplate<MyMovies,String> elasticsearchTemplate;

    @Test
    public void testSave() throws Exception {
        MyMovies myMovies = new MyMovies();
        myMovies.setId("3");
        myMovies.setTitle("Titanic");
        Actors actors = new Actors();
        actors.setFirst_name("DiCaprio1");
        actors.setLast_name("Leonardo1");

        Actors actors2 = new Actors();
        actors2.setFirst_name("DiCaprio2");
        actors2.setLast_name("Leonardo2");

        List<Actors> list = new ArrayList<>();
        list.add(actors);
        list.add(actors2);
        myMovies.setActors(list);
        elasticsearchTemplate.save(myMovies);
    }

    @Test
    public void testSearch() throws Exception {
        NestedQueryBuilder queryBuilder = QueryBuilders.nestedQuery("actors",QueryBuilders.matchQuery("actors.first_name","DiCaprio1"), ScoreMode.Total);
        elasticsearchTemplate.search(queryBuilder, MyMovies.class).forEach(s -> {System.out.println(s);});
    }


    @Test
    public void testPhraseSuggester() throws Exception {
        System.out.println("#######");
        ElasticsearchTemplateImpl.PhraseSuggestParam param
                = new ElasticsearchTemplateImpl.PhraseSuggestParam(5,1,null,"always");
        elasticsearchTemplate.phraseSuggest("title", "功",param, MyMovies.class).forEach(s -> System.out.println(s));
    }

    @Test
    public void testAggs() throws Exception {
        Map map = elasticsearchTemplate.aggs("title", AggsType.count,null, MyMovies.class,"title");
        map.forEach((k,v) -> System.out.println(k+"     "+v));
    }
}
