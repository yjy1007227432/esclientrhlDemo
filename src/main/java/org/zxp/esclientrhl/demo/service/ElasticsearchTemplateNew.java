package org.zxp.esclientrhl.demo.service;


import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.reindex.ReindexRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;
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
