package org.zxp.esclientrhl.demo.domain;


import lombok.Data;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * es查询辅助类
 *
 * @Author lix
 * @Date 2021/1/14
 */
@Data
public class ESQueryHelper {

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private BoolQueryBuilder bool;
    private QueryBuilder condition;
    private SearchRequest searchRequest;


    private ESQueryHelper(String... indices) {
        searchRequest = new SearchRequest(indices);
        this.bool = new BoolQueryBuilder();
    }

    /**
     * 初始化方法
     *
     * @return
     */
    public static ESQueryHelper build(String... indic) {
        return new ESQueryHelper(indic);
    }

    /**
     * 返回 Request
     *
     * @return
     */
    public SearchRequest getRequest() {
        return this.searchRequest;
    }

    /**
     * 拼接and条件
     *
     * @param queryBuilder 条件
     * @return
     */
    public ESQueryHelper and(QueryBuilder queryBuilder) {
        bool.must(queryBuilder);
        return this;
    }

    /**
     * 拼接or条件
     *
     * @param queryBuilder
     * @return
     */
    public ESQueryHelper or(QueryBuilder queryBuilder) {
        bool.should(queryBuilder);
        return this;
    }


    public ESQueryHelper orNew(List<SearchParam.Or> ors) {
        BoolQueryBuilder shouldQ= QueryBuilders.boolQuery();
        ors.forEach(or -> {
            switch (or.getType()){
                case "equals":
                case "in":
                    shouldQ.should(QueryBuilders.termQuery(or.getColumn(),or.getVal().toString()));break;
                case "like":shouldQ.should(QueryBuilders.wildcardQuery(or.getColumn(),"*".concat(or.getVal().toString()).concat("*")));break;
                case "leftLike":shouldQ.should(QueryBuilders.termQuery(or.getColumn(),"*".concat(or.getVal().toString())));break;
                case "rightLike":shouldQ.should(QueryBuilders.termQuery(or.getColumn(),or.getVal().toString().concat("*")));break;
                case "gt":shouldQ.should(QueryBuilders.rangeQuery(or.getColumn()).gt(or.getVal()));break;
                case "lt":shouldQ.should(QueryBuilders.rangeQuery(or.getColumn()).lt(or.getVal()));break;
                case "between":shouldQ.should(QueryBuilders.rangeQuery(or.getColumn()).gt(or.getVal().toString().split(",")[0]).lte(or.getVal().toString().split(",")[1]));;break;
            }
        });
        bool.must(shouldQ);
        return this;
    }

    /**
     * 拼接过滤条件。根据查询条件去查询文档，不去计算分数，而且filter会对经常被过滤的数据进行缓存
     *
     * @param queryBuilder
     * @return
     */
    public ESQueryHelper filter(QueryBuilder queryBuilder) {
        bool.filter(queryBuilder);
        return this;
    }

    /**
     * 拼接不等于条件
     *
     * @param queryBuilder
     * @return
     */
    public ESQueryHelper not(QueryBuilder queryBuilder) {
        bool.mustNot(queryBuilder);
        return this;
    }

    /**
     * 类似于sql：column like ‘%val%’
     *
     * @param column
     * @param val
     * @return
     */
    public static QueryBuilder like(String column, String val) {
        return QueryBuilders.wildcardQuery(column, "*".concat(val).concat("*"));
    }

    /**
     * 类似于sql：column like ‘%val’
     *
     * @param column
     * @param val
     * @return
     */
    public static QueryBuilder leftLike(String column, String val) {
        return QueryBuilders.wildcardQuery(column, "*".concat(val));
    }

    /**
     * 类似于sql：column like ‘val%’
     *
     * @param column
     * @param val
     * @return
     */
    public static QueryBuilder rightLike(String column, String val) {
        return QueryBuilders.wildcardQuery(column, val.concat("*"));
    }

    /**
     * 大于
     *
     * @param column
     * @param val
     * @return
     */
    public static QueryBuilder gt(String column, Object val) {
        return QueryBuilders.rangeQuery(column).gt(val);
    }

    /**
     * 小于
     *
     * @param column
     * @param val
     * @return
     */
    public static QueryBuilder lt(String column, Object val) {
        return QueryBuilders.rangeQuery(column).lt(val);
    }

    /**
     * 在 from 到 to之间，即 from < 查新内容 < to
     *
     * @param column 查询字段，如：age
     * @param from   如：5
     * @param to     如：20
     * @return
     */
    public static QueryBuilder between(String column, Object from, Object to) {
        return QueryBuilders.rangeQuery(column).gt(from).lte(to);
    }

    /**
     * 等于
     *
     * @param column
     * @param val
     * @return
     */
    public static QueryBuilder equals(String column, Object val) {
        return QueryBuilders.termQuery(column, val);
    }

    /**
     * 相当于mysql的in，即 column in(val1, val2....)
     *
     * @param column
     * @param val
     * @return
     */
    public static QueryBuilder in(String column, Object... val) {
        return QueryBuilders.termsQuery(column, val);
    }

    /**
     * 相当于mysql的in，即 column in(val1, val2....)
     *
     * @param column
     * @param inList
     * @return
     */
    public static QueryBuilder in(String column, Collection<Object> inList) {
        return QueryBuilders.termsQuery(column, inList);
    }

    /**
     * match 查询属于高级查询，会根据你查询字段的类型不一样，采用不同的查询方式.
     * 查询的是日期或者数值，他会将你基于字符串的查询内容转换为日期或数值对待.
     * 如果查询的内容是一个不能被分词的内容（keyword）,match 不会将你指定的关键字进行分词.
     * 如果查询的内容是一个可以被分词的内容（text）,match 查询会将你指定的内容根据一定的方式进行分词，去分词库中匹配指定的内容.
     * match 查询，实际底层就是多个term 查询，将多个term查询的结果给你封装到一起.
     *
     * @param column
     * @param text
     * @return
     */
    public static QueryBuilder match(String column, Object text) {
        return QueryBuilders.matchQuery(column, text);
    }

    /**
     * multiMatchQuery 针对多个field 进行检索，多个field对应一个文本。
     * <p>
     * 如：
     * text：北京, field: area, hometown 表示为搜索area含北京或hometown含北京的
     * </p>
     *
     * @param text
     * @param field
     * @return
     */
    public static QueryBuilder multiMatchQuery(Object text, String... field) {
        return QueryBuilders.multiMatchQuery(text, field);
    }


    /**
     * 根据_id查询
     *
     * @param ids 批量id
     * @return
     */
    public static QueryBuilder ids(String... ids) {
        return QueryBuilders.idsQuery().addIds(ids);
    }

    /**
     * 正则匹配。效率低下，非特殊情况不建议使用
     *
     * @param rgx 正则表达式
     * @return
     */
    public static QueryBuilder regexp(String field, String rgx) {
        return QueryBuilders.regexpQuery(field, rgx);
    }

    /**
     * 分页，相当于mysql的limit from, size
     *
     * @param from
     * @param size
     * @return
     */
    public ESQueryHelper limit(Integer from, Integer size) {
        this.searchRequest.source().from(from);
        this.searchRequest.source().size(size);
        return this;
    }

    /**
     * 从结果的第from条取
     *
     * @param from
     * @return
     */
    public ESQueryHelper from(Integer from) {
        this.searchRequest.source().from(from);
        return this;
    }

    /**
     * 从结果集获取size条
     *
     * @param size
     * @return
     */
    public ESQueryHelper size(Integer size) {
        this.searchRequest.source().size(size);
        return this;
    }

    /**
     * 按字段field倒序
     *
     * @param field
     * @return
     */
    public ESQueryHelper desc(String field) {
        this.searchRequest.source().sort(field, SortOrder.DESC);
        return this;
    }

    /**
     * 按字段field正序
     *
     * @param field
     * @return
     */
    public ESQueryHelper asc(String field) {
        this.searchRequest.source().sort(field, SortOrder.ASC);
        return this;
    }


    /**
     * 高亮某些字段
     * @param field
     * @param preTags
     * @param postTags
     * @return
     */
    public ESQueryHelper highlighter(String field,String preTags,String postTags) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.field(field, 10)
                .preTags(Optional.ofNullable(preTags).orElse("<font color='yellow'>"))
                .postTags((Optional.ofNullable(postTags).orElse("</font>")));
        this.searchRequest.source().highlighter(highlightBuilder);
        return this;
    }

    /**
     * 自定义查询条件的时候，如果设置该值不会再执复合查询
     *
     * @param queryBuilder
     * @return
     */
    public ESQueryHelper searchCondition(QueryBuilder queryBuilder) {
        this.condition = queryBuilder;
        return this;
    }

    /**
     * 执行查询
     *
     * @param client
     * @return
     * @throws IOException
     */
    public SearchResponse execute(RestHighLevelClient client) throws IOException {
        return this.execute(client, RequestOptions.DEFAULT);
    }

    /**
     * 执行查询
     *
     * @param client
     * @param requestOptions
     * @return
     * @throws IOException
     */
    public SearchResponse execute(RestHighLevelClient client, RequestOptions requestOptions) throws IOException {
        try {
            this.searchRequest.source().query(condition != null ? condition : bool);
            return client.search(this.searchRequest, requestOptions);
        } catch (IOException e) {
            logger.error("{}执行查询方法时发生异常: {}", this.getClass().getName(), e.getMessage());
            throw e;
        }
    }


    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = null; // 你的es客户端对象
        SearchResponse searchResponse = ESQueryHelper.build("你的index")
                .and(ESQueryHelper.equals("name", "张三")) // 名字为张三
                .and(ESQueryHelper.between("age", 18, 28)) // 年龄在18~28之间
                .not(ESQueryHelper.equals("gender", "女")) // 不能是女性
                .or(ESQueryHelper.like("area", "北京"))// 或者是北京人
                .size(10) // 只查询10条
                .execute(client);// 异常自己处理
        // searchResponse.getHits().getHits() 结果自己处理
    }
}

