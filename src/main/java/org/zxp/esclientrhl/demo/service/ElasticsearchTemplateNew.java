package org.zxp.esclientrhl.demo.service;


import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.*;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
import org.zxp.esclientrhl.demo.domain.ESQueryHelper;
import org.zxp.esclientrhl.demo.enums.DataTypeNew;
import org.zxp.esclientrhl.demo.response.SqlResponse;
import org.zxp.esclientrhl.demo.util.DateUtil;
import org.zxp.esclientrhl.enums.DataType;
import org.zxp.esclientrhl.enums.SqlFormat;
import org.zxp.esclientrhl.repository.ElasticsearchTemplateImpl;
import org.zxp.esclientrhl.util.JsonUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


@Component
@Slf4j
public class ElasticsearchTemplateNew<T, M> extends ElasticsearchTemplateImpl<T, M> {

    @Autowired
    RestHighLevelClient client;


    public List<T> queryBySQL(String sql, Class<T> clazz) throws Exception {
        String s = queryBySQL(sql, SqlFormat.JSON);
        SqlResponse sqlResponse = JsonUtils.string2Obj(s, SqlResponse.class);
        List<T> result = new ArrayList<>();
        if(sqlResponse != null && !CollectionUtils.isEmpty(sqlResponse.getRows())){
            for (List<String> row : sqlResponse.getRows()) {
                result.add(generateObjBySQLReps(sqlResponse.getColumns(),row,clazz));
            }
        }
        return result;
    }


    /**
     * 添加别名
     * @param indexName
     * @param aliasName
     * @return
     * @throws IOException
     */
    public Boolean addAlias(String indexName, String aliasName) throws IOException {
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.ADD)
                        .index(indexName)
                        .alias(aliasName);
        aliasesRequest.addAliasAction(aliasAction);
        AcknowledgedResponse acknowledgedResponse = client.indices().updateAliases(aliasesRequest,RequestOptions.DEFAULT);
        return acknowledgedResponse.isAcknowledged();
    }


    /**
     * 删除别名
     * @param indexName
     * @param aliasName
     * @return
     * @throws IOException
     */
    public Boolean dropAlias(String indexName, String aliasName) throws IOException {
        IndicesAliasesRequest aliasesRequest = new IndicesAliasesRequest();
        IndicesAliasesRequest.AliasActions aliasAction =
                new IndicesAliasesRequest.AliasActions(IndicesAliasesRequest.AliasActions.Type.REMOVE)
                        .index(indexName)
                        .alias(aliasName);
        aliasesRequest.addAliasAction(aliasAction);
        AcknowledgedResponse acknowledgedResponse = client.indices().updateAliases(aliasesRequest,RequestOptions.DEFAULT);
        return acknowledgedResponse.isAcknowledged();
    }

    /**
     * 重建索引后修改别名
     *
     * @param aliasname
     * @param oldIndexname
     * @param newIndexname
     * @return
     */
    public Boolean changeAliasAfterReindex(String aliasname, String oldIndexname, String newIndexname) throws IOException {
        IndicesAliasesRequest.AliasActions addIndexAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.ADD).index(newIndexname).alias(aliasname);
        IndicesAliasesRequest.AliasActions removeAction = new IndicesAliasesRequest.AliasActions(
                IndicesAliasesRequest.AliasActions.Type.REMOVE).index(oldIndexname).alias(aliasname);

        IndicesAliasesRequest indicesAliasesRequest = new IndicesAliasesRequest();
        indicesAliasesRequest.addAliasAction(addIndexAction);
        indicesAliasesRequest.addAliasAction(removeAction);
        AcknowledgedResponse indicesAliasesResponse = client.indices().updateAliases(indicesAliasesRequest,
                RequestOptions.DEFAULT);
        return indicesAliasesResponse.isAcknowledged();
    }




    /**
     * 重建索引，拷贝数据
     *
     * @param oldIndexname
     * @param newIndexname
     */
    public void reindex(String oldIndexname, String newIndexname) throws IOException {
        ReindexRequest request = new ReindexRequest();
        request.setSourceIndices(oldIndexname);
        request.setDestIndex(newIndexname);
        request.setSourceBatchSize(1000);
        request.setDestOpType("create");
        request.setConflicts("proceed");
//        request.setScroll(TimeValue.timeValueMinutes(10));
//        request.setTimeout(TimeValue.timeValueMinutes(20));
        request.setRefresh(true);
        client.reindex(request, RequestOptions.DEFAULT);
    }

    /**
     * Metric Aggregation ES指标聚合
     * avg 平均值
     * max 最大值
     * min 最小值
     * sum 和
     * value_count 数量
     * cardinality 基数（distinct去重）
     * stats 包含avg,max,min,sum和count
     * @param index
     * @param field
     * @param esQueryHelper
     * @return
     * @throws IOException
     */


    public  Map<String, Object> aggMetric(String index, String field, ESQueryHelper esQueryHelper) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String type = "stats";
        switch (type){
//            case "sum":SumAggregationBuilder sumAggregationBuilder = AggregationBuilders.sum(type).field(field);
//                       searchSourceBuilder.aggregation(sumAggregationBuilder); break;
//            case "avg":AvgAggregationBuilder avgAggregationBuilder = AggregationBuilders.avg(type).field(field);
//                       searchSourceBuilder.aggregation(avgAggregationBuilder); break;
//            case "max":MaxAggregationBuilder maxAggregationBuilder = AggregationBuilders.max(type).field(field);
//                       searchSourceBuilder.aggregation(maxAggregationBuilder); break;
//            case "min":MinAggregationBuilder minAggregationBuilder = AggregationBuilders.min(type).field(field);
//                       searchSourceBuilder.aggregation(minAggregationBuilder); break;
//            case "count":ValueCountAggregationBuilder valueCountAggregationBuilder = AggregationBuilders.count(type).field(field);
//                         searchSourceBuilder.aggregation(valueCountAggregationBuilder); break;
            case "cardinality ":CardinalityAggregationBuilder cardinalityAggregationBuilder = AggregationBuilders.cardinality(type).field(field);
                                searchSourceBuilder.aggregation(cardinalityAggregationBuilder); break;
            case "stats ":StatsAggregationBuilder statsAggregationBuilder = AggregationBuilders.stats(type).field(field);
                searchSourceBuilder.aggregation(statsAggregationBuilder); break;
        }
        /**
         * 不输出原始数据
         */
        searchSourceBuilder.query(esQueryHelper.getBool());
        searchSourceBuilder.size(0);
        /**
         * 打印dsl语句
         */
        log.info("dsl:" + searchSourceBuilder.toString());
        /**
         * 设置索引以及填充语句
         */
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(searchSourceBuilder);
        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        /**
         * 解析数据，获取tag_tr的指标聚合参数。
         */
        ParsedStats premium = response.getAggregations().get("stats");
        Map<String,Object> result = new HashMap<>();
        result.put("max",premium.getMax());
        result.put("min",premium.getMin());
        result.put("avg",premium.getAvg());
        result.put("count",premium.getCount());
        result.put("sum",premium.getSum());
        return result;
    }

    /**
     * 使用field字段进行桶分组聚合
     * @param index
     * @param field
     * @param esQueryHelper
     * @return
     * @throws IOException
     */

    public  Map<String,Long> aggTerms(String index, String field, ESQueryHelper esQueryHelper) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        /**
         * 使用field字段进行桶分组
         * 可以使用sum、avg进行指标聚合
         */
        TermsAggregationBuilder aggregationBuilder = AggregationBuilders.
                terms("aggTerms").field(field); //求平均值

        searchSourceBuilder.aggregation(aggregationBuilder);
        /**
         * 不输出原始数据
         */
        searchSourceBuilder.size(0);
        /**
         * 打印dsl语句
         */
        log.info("dsl:" + searchSourceBuilder.toString());
        /**
         * 设置索引以及填充语句
         */
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(index);
        searchRequest.source(searchSourceBuilder);

        SearchResponse response = client.search(searchRequest, RequestOptions.DEFAULT);
        /**
         * 解析数据，获取tag_tr的指标聚合参数。
         */
        Aggregations aggregations = response.getAggregations();
        ParsedStringTerms parsedStringTerms = aggregations.get("tag_tr");
        List<? extends Terms.Bucket> buckets = parsedStringTerms.getBuckets();
        Map<String,Long> result = new HashMap<>();
        for (Terms.Bucket bucket : buckets) {
            //key的数据
            String key = bucket.getKey().toString();
            long docCount = bucket.getDocCount();
            //获取数据
            Aggregations bucketAggregations = bucket.getAggregations();
//            ParsedSum sumId = bucketAggregations.get("sum_id");
//            ParsedValueCount avgId = bucketAggregations.get("avg_id");
//            System.out.println(key + ":" + docCount + "-" + sumId.getValue() + "-" + avgId.getValue());
            result.put(key,docCount);
//            System.out.println(key + ":" + docCount + "-"+avgId.getValueAsString());
        }
        return result;
    }


    private <T> T generateObjBySQLReps(List<SqlResponse.ColumnsDTO> columns,List<String> rows,Class<T> clazz) throws Exception {
        if(rows.size() != columns.size()){
            throw new Exception("sql column not match");
        }
        Map<String, NameTypeValueMap> valueMap = new HashMap();
        for (int i = 0; i < rows.size(); i++) {
            NameTypeValueMap m = new NameTypeValueMap();
            m.setDataType(DataTypeNew.getDataTypeByStr(columns.get(i).getType()));
            m.setFieldName(columns.get(i).getName());
            m.setValue(rows.get(i));
            valueMap.put(columns.get(i).getName(),m);
        }
        T t = (T)typeMapToObject(valueMap, clazz);
        return t;
    }

    public static class NameTypeValueMap{
        private String fieldName;
        private DataTypeNew dataType;
        private String value;

        public String getFieldName() {
            return fieldName;
        }

        public void setFieldName(String fieldName) {
            this.fieldName = fieldName;
        }

        public DataTypeNew getDataType() {
            return dataType;
        }

        public void setDataType(DataTypeNew dataType) {
            this.dataType = dataType;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    public static <T> T typeMapToObject(Map<String,NameTypeValueMap> map, Class<T> beanClass) throws Exception {
        if (map == null)
            return null;
        T t = beanClass.newInstance();
        Field[] fields = t.getClass().getDeclaredFields();
        for (Field field : fields) {
            NameTypeValueMap nameTypeValueMap = map.get(field.getName());
            if(map.get(field.getName()) == null || nameTypeValueMap == null ){
                continue;
            }
            int mod = field.getModifiers();
            if(Modifier.isStatic(mod) || Modifier.isFinal(mod)){
                continue;
            }
            field.setAccessible(true);
            if(nameTypeValueMap.getDataType() == DataTypeNew.date_type){
                field.set(t, DateUtil.strToDate(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.double_type){
                field.set(t, Double.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.byte_type){
                field.set(t, Byte.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.boolean_type){
                field.set(t, Boolean.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.integer_type){
                field.set(t, Integer.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.float_type){
                field.set(t, Float.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.long_type){
                field.set(t, Long.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.keyword_type){
                field.set(t, String.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.text_type){
                field.set(t, String.valueOf(nameTypeValueMap.getValue()));
            } else if(nameTypeValueMap.getDataType() == DataTypeNew.short_type){
                field.set(t, Short.valueOf(nameTypeValueMap.getValue()));
            } else{
                throw new Exception("not support field type covert");
            }
        }
        return t;
    }



}
