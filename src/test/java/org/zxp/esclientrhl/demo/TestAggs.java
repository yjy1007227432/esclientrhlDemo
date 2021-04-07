package org.zxp.esclientrhl.demo;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.Stats;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.ValueCount;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.zxp.esclientrhl.demo.domain.IndexDemo;
import org.zxp.esclientrhl.enums.AggsType;
import org.zxp.esclientrhl.repository.Down;
import org.zxp.esclientrhl.repository.ElasticsearchTemplate;

import java.util.List;
import java.util.Map;

/**
 * @program: 聚合的使用
 * @description: ${description}
 * @author: X-Pacific zhang
 * @create: 2019-02-27 10:09
 **/
public class TestAggs extends  EsclientrhlDemoApplicationTests {
    @Autowired
    ElasticsearchTemplate<IndexDemo,String> elasticsearchTemplate;

    /**
     * 通过官方自带api AggregationBuilders 进行聚合查询
     * @throws Exception
     */
    @Test
    public void testOri() throws Exception {
        //https://www.elastic.co/guide/en/elasticsearch/client/java-api/6.6/_metrics_aggregations.html
        SumAggregationBuilder aggregation = AggregationBuilders.sum("agg").field("sum_amount");
        Aggregations aggregations = elasticsearchTemplate.aggs(aggregation,null,IndexDemo.class);
        Sum agg = aggregations.get("agg");
        double value = agg.getValue();
        System.out.println(value);
    }


    /**
     * 5种聚合函数的查询
     * @throws Exception
     */
    @Test
    public void testAggs() throws Exception {
        double sum = elasticsearchTemplate.aggs("sum_premium", AggsType.sum,null,IndexDemo.class);
        double count = elasticsearchTemplate.aggs("sum_premium", AggsType.count,null,IndexDemo.class);
        double avg = elasticsearchTemplate.aggs("sum_premium", AggsType.avg,null,IndexDemo.class);
        double min = elasticsearchTemplate.aggs("sum_premium", AggsType.min,null,IndexDemo.class);
        double max = elasticsearchTemplate.aggs("sum_premium", AggsType.max,null,IndexDemo.class);
        System.out.println("sum===="+sum);
        System.out.println("count===="+count);
        System.out.println("avg===="+avg);
        System.out.println("min===="+min);
        System.out.println("max===="+max);
    }


    /**
     * group by appli_name
     * @throws Exception
     */
    @Test
    public void testAggs2() throws Exception {
        Map map = elasticsearchTemplate.aggs("sum_premium", AggsType.sum,null,IndexDemo.class,"appli_name");
        map.forEach((k,v) -> System.out.println(k+"     "+v));
    }


    /**
     * group by appli_name,risk_code
     * @throws Exception
     */
    @Test
    public void testAggs2level() throws Exception {
        String[] strs = {"appli_name","risk_code"};
        List<Down> list = elasticsearchTemplate.aggswith2level("sum_premium", AggsType.sum,null,IndexDemo.class,strs);
        list.forEach(down ->
            {
                System.out.println("1:"+down.getLevel_1_key());
                System.out.println("2:"+down.getLevel_2_key() + "    "+ down.getValue());
            }
        );
    }


    /**
     * Stats
     * @throws Exception
     */
    @Test
    public void testAggsStats2() throws Exception {
        Map<String,Stats> stats = elasticsearchTemplate.statsAggs("sum_premium",null,IndexDemo.class,"risk_code");
        stats.forEach((k,v) ->
            {
                System.out.println(k+"    count:"+v.getCount()+" sum:"+v.getSum()+"...");
            }
        );
    }

    /**
     * 基数查询
     * @throws Exception
     */
    @Test
    public void testCardinality() throws Exception {
        long value = elasticsearchTemplate.cardinality("proposal_no",null,IndexDemo.class);
        System.out.println(value);
    }

    /**
     * 以百分比聚合
     * @throws Exception
     */
    @Test
    public void testPercentiles() throws Exception {
        Map map = elasticsearchTemplate.percentilesAggs("sum_premium",null,IndexDemo.class);
        map.forEach((k,v) ->
                {
                    System.out.println(k+"     "+v);
                }
        );
        double[] dbs = {10.0,20.0,30.0,50.0,60.0,90.0,99.0};
        Map map2 = elasticsearchTemplate.percentilesAggs("sum_premium",null,IndexDemo.class,dbs);
    }


    /**
     * 以百分等级聚合 (统计在多少数值之内占比多少)
     * @throws Exception
     */
    @Test
    public void testPercentilesRank() throws Exception {
        double[] dbs = {1,4,5,9};
        Map map = elasticsearchTemplate.percentileRanksAggs("sum_premium",null,IndexDemo.class,dbs);
        map.forEach((k,v) ->
                {
                    System.out.println(k+"     "+v);
                }
        );
    }

    /**
     * 过滤器聚合
     * @throws Exception
     */
    @Test
    public void testFilterAggs() throws Exception {
        FiltersAggregator.KeyedFilter[] filters = {new FiltersAggregator.KeyedFilter("0101", QueryBuilders.matchPhraseQuery("risk_code", "0101")),
                new FiltersAggregator.KeyedFilter("0103", QueryBuilders.matchQuery("risk_code", "0103"))};
        Map map = elasticsearchTemplate.filterAggs("sum_premium", AggsType.sum, null,IndexDemo.class,filters);
        map.forEach((k, v) ->
                System.out.println(k + "    " + v)
        );
    }

    /**
     * 直方图聚合
     * @throws Exception
     */
    @Test
    public void testHistogramAggs() throws Exception {
        Map map = elasticsearchTemplate.histogramAggs("proposal_no", AggsType.count, null,IndexDemo.class,"sum_premium",3);
        map.forEach((k, v) ->
                System.out.println(k + "    " + v)
        );
    }

    /**
     * 日期直方图聚合
     * @throws Exception
     */
    @Test
    public void testDateHistogramAggs() throws Exception {
        Map map = elasticsearchTemplate.dateHistogramAggs("sum_premium", AggsType.sum, null,IndexDemo.class,"input_date", DateHistogramInterval.hours(2));
        map.forEach((k, v) ->
                System.out.println(k + "    " + v)
        );
    }

    /**
     * 范围聚合
     * @throws Exception
     */
    @Test
    public void testRangeAggs() throws Exception {
        AggregationBuilder aggregation =
                AggregationBuilders.range("range").field("sum_premium").addUnboundedTo(1).addRange(1,4).addRange(4,100).addUnboundedFrom(100);
        aggregation.subAggregation(AggregationBuilders.count("agg").field("proposal_no.keyword"));
        Aggregations aggregations = elasticsearchTemplate.aggs(aggregation,null,IndexDemo.class);
        Range range = aggregations.get("range");
        for (Range.Bucket entry : range.getBuckets()) {
            ValueCount count = entry.getAggregations().get("agg");
            long value = count.getValue();
            System.out.println(entry.getKey() + "    " + value);
        }
    }
}
