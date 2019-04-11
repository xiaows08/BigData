package cn.xiaows.studyelasticsearch5.util;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.sort.SortOrder;

/**
 * 查询参数工具类
 *
 * @author XIAOWANGSHENG
 */
public class SearchParams {
    private String index;
    private String type;
    private QueryBuilder queryBuilder;
    private AggregationBuilder aggregationBuilder;
    private Integer from;
    private Integer size;
    private String[] includes;
    private String[] excludes;
    private String orderField;
    private SortOrder order;

    public SearchParams() {
        this.from = 0;
        this.size = 1000;
        this.includes = null;
        this.excludes = new String[0];
        this.orderField = "_score";
        this.order = SortOrder.ASC;
    }

    public String getIndex() {
        return index;
    }

    public SearchParams setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getType() {
        return type;
    }

    public SearchParams setType(String type) {
        this.type = type;
        return this;
    }

    public QueryBuilder getQueryBuilder() {
        return queryBuilder;
    }

    public SearchParams setQueryBuilder(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    public AggregationBuilder getAggregationBuilder() {
        return aggregationBuilder;
    }

    public SearchParams setAggregationBuilder(AggregationBuilder aggregationBuilder) {
        this.aggregationBuilder = aggregationBuilder;
        return this;
    }

    public Integer getFrom() {
        return from;
    }

    public SearchParams setFrom(Integer from) {
        this.from = from;
        return this;
    }

    public Integer getSize() {
        return size;
    }

    public SearchParams setSize(Integer size) {
        this.size = size;
        return this;
    }

    public String[] getIncludes() {
        return includes;
    }

    public SearchParams setIncludes(String... includes) {
        this.includes = includes;
        return this;
    }

    public String[] getExcludes() {
        return excludes;
    }

    public SearchParams setExcludes(String... excludes) {
        this.excludes = excludes;
        return this;
    }

    public String getOrderField() {
        return orderField;
    }

    public SearchParams setOrderField(String orderField) {
        this.orderField = orderField;
        return this;
    }

    public SortOrder getOrder() {
        return order;
    }

    public SearchParams setOrder(SortOrder order) {
        this.order = order;
        return this;
    }
}
