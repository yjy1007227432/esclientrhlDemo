package org.zxp.esclientrhl.demo;

import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FieldValueFactorFunction;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.junit.jupiter.api.Test;


/**
 * @program: 更多QueryBuilder详见 https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.6/java-rest-high-query-builders.html
 * @description:
 * @author: X-Pacific zhang
 * @create: 2021-04-07 13:56
 **/
public class TestQueryBuilder extends EsclientrhlDemoApplicationTests {
    @Test
    public void testTermQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.termQuery("appli_name.keyword","456");
    }

    @Test
    public void testMatchPhraseQuery() throws Exception {
        //中国好男儿
        QueryBuilder queryBuilder = QueryBuilders.matchPhraseQuery("appli_name","中国");
        QueryBuilder queryBuilder2 = QueryBuilders.matchPhraseQuery("appli_name","中男").slop(1);
    }

    @Test
    public void testRangeQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.rangeQuery("sum_premium").from(1).to(3);
    }

    @Test
    public void testMatchQuery() throws Exception {
        QueryBuilder queryBuilder1 = QueryBuilders.matchQuery("appli_name","中男儿");
        QueryBuilder queryBuilder2 = QueryBuilders.matchQuery("appli_name","spting");
        ((MatchQueryBuilder) queryBuilder2).fuzziness(Fuzziness.AUTO);
        QueryBuilder queryBuilder3 = QueryBuilders.matchQuery("appli_name","spring sps").operator(Operator.AND);
        QueryBuilder queryBuilder4 = QueryBuilders.matchQuery("appli_name","中 男 儿 美 丽 人 生").minimumShouldMatch("75%");
        QueryBuilder queryBuilder5 = QueryBuilders.fuzzyQuery("appli_name","spting");
    }

    @Test
    public void testQueryBuilderBoost() throws Exception {
        QueryBuilder queryBuilder1 = QueryBuilders.termQuery("appli_name.keyword","spring").boost(5);
        QueryBuilder queryBuilder2 = QueryBuilders.termQuery("appli_name.keyword","456").boost(3);
        QueryBuilder queryBuilder3 = QueryBuilders.termQuery("appli_name.keyword","123");
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.should(queryBuilder1).should(queryBuilder2).should(queryBuilder3);
    }

    @Test
    public void testBoolQueryBuilder() throws Exception {
        QueryBuilder queryBuilder1 = QueryBuilders.termQuery("appli_name.keyword","spring");
        QueryBuilder queryBuilder2 = QueryBuilders.termQuery("appli_name.keyword","456");
        QueryBuilder queryBuilder3 = QueryBuilders.termQuery("risk_code","0101");
        QueryBuilder queryBuilder4 = QueryBuilders.termQuery("proposal_no.keyword","1234567");
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery();
        queryBuilder.should(queryBuilder1).should(queryBuilder2);
        queryBuilder.must(queryBuilder3);
        queryBuilder.mustNot(queryBuilder4);
    }

    @Test
    public void testPrefixQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.prefixQuery("appli_name","1");
    }

    @Test
    public void testWildcardQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.wildcardQuery("appli_name","1?3");
    }

    @Test
    public void testRegexpQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.regexpQuery("appli_name","[0-9].+");
    }



    @Test
    public void testFilterQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.termQuery("appli_name.keyword","456"))
                .filter(QueryBuilders.matchPhraseQuery("risk_code","0101"));
    }

    @Test
    public void testDisMaxQuery() throws Exception {
        QueryBuilders.disMaxQuery()
                .add(QueryBuilders.matchQuery("title", "bryant fox"))
                .add(QueryBuilders.matchQuery("body", "bryant fox"))
                .tieBreaker(0.2f);
    }

    @Test
    public void testMultiMatchQuery() throws Exception {
        QueryBuilders.multiMatchQuery("Quick pets", "title","body")
                .minimumShouldMatch("20%")
                .type(MultiMatchQueryBuilder.Type.BEST_FIELDS)
                .tieBreaker(0.2f);

        QueryBuilders.multiMatchQuery("shanxi datong", "s1","s2","s3","s4")
                .type(MultiMatchQueryBuilder.Type.MOST_FIELDS);


        QueryBuilders.multiMatchQuery("chengdu sichuan", "s1","s2","s3","s4")
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS);
    }

    @Test
    public void testFunctionScoreQuery() throws Exception {
        //新的算分 = 老的算分 * log( 1 + factor*votes的值)
        ScoreFunctionBuilder<?> scoreFunctionBuilder = ScoreFunctionBuilders
                .fieldValueFactorFunction("votes")
                .modifier(FieldValueFactorFunction.Modifier.LOG1P)
                .factor(0.1f);
        QueryBuilders.functionScoreQuery(QueryBuilders.matchQuery("title", "bryant fox"),scoreFunctionBuilder)
                .boostMode(CombineFunction.MULTIPLY)//默认就是乘
                .maxBoost(3f);
    }

    @Test
    public void testBoostingQuery() throws Exception {
        QueryBuilder queryBuilder = QueryBuilders.boostingQuery(QueryBuilders.matchQuery("title", "bryant fox"),
                QueryBuilders.matchQuery("flag", "123")).negativeBoost(0.2f);
    }
}
