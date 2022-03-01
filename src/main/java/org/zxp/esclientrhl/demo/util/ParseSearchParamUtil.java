package org.zxp.esclientrhl.demo.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.elasticsearch.index.query.QueryBuilders;
import org.zxp.esclientrhl.demo.domain.ESQueryHelper;
import org.zxp.esclientrhl.demo.domain.SearchParam;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class ParseSearchParamUtil {
    public static ESQueryHelper ParseSearchParamUtil(String searchParamJson,String index){
        List<SearchParam> searchParams = JSON.parseArray(searchParamJson, SearchParam.class);

        ESQueryHelper esQueryHelper = ESQueryHelper.build(index);

        searchParams.forEach(searchParam -> {
            if("and".equals(searchParam.getOperator())) {
                switch (searchParam.getType()) {
                    case "equals":esQueryHelper.and(ESQueryHelper.equals(searchParam.getColumn(),searchParam.getVal()));break;
                    case "in":Collection<Object> list = Arrays.asList(searchParam.getVal().toString().split(","));
                              esQueryHelper.and(ESQueryHelper.in(searchParam.getColumn(),list));break;
                    case "like":esQueryHelper.and(ESQueryHelper.like(searchParam.getColumn(),searchParam.getVal().toString()));break;
                    case "leftLike":esQueryHelper.and(ESQueryHelper.leftLike(searchParam.getColumn(),searchParam.getVal().toString()));break;
                    case "rightLike":esQueryHelper.and(ESQueryHelper.rightLike(searchParam.getColumn(),searchParam.getVal().toString()));break;
                    case "gt":esQueryHelper.and(ESQueryHelper.gt(searchParam.getColumn(),searchParam.getVal().toString()));break;
                    case "lt":esQueryHelper.and(ESQueryHelper.lt(searchParam.getColumn(),searchParam.getVal().toString()));break;
                    case "between": esQueryHelper.and(QueryBuilders.rangeQuery(searchParam.getColumn()).gt(searchParam.getVal().toString().split(",")[0]).lte(searchParam.getVal().toString().split(",")[1]));break;
                    case "match":esQueryHelper.and(ESQueryHelper.match(searchParam.getColumn(),searchParam.getVal()));break;
                    case "matchPhrase":esQueryHelper.and(ESQueryHelper.matchPhrase(searchParam.getColumn(),searchParam.getVal()));break;
                }
            }else {
                esQueryHelper.orNew(searchParam.getOrs());
            }
        });
        return esQueryHelper;
    }
}
