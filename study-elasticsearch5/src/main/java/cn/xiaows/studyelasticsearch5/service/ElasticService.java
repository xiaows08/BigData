package cn.xiaows.studyelasticsearch5.service;

import cn.xiaows.studyelasticsearch5.util.BaseQueryUtils;
import cn.xiaows.studyelasticsearch5.util.SearchParams;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import java.util.Map;

@Slf4j
@Repository
public class ElasticService extends BaseQueryUtils {

    @Autowired
    private TransportClient client;

    @Value("${customer.file.json.path}")
    private String path;

    public long readProbAgg(String date, String name) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("complaint_dt", date));
        if (name != null) {
            queryBuilder.must(QueryBuilders.termQuery("family_name", name));
        }
        SearchParams searchParams = new SearchParams()
                .setIndex("ic")
                .setType("T_KEYWORD_EXTRACT")
                .setQueryBuilder(queryBuilder)
                .setOrderField("timestamp")
                .setOrder(SortOrder.DESC)
                .setFrom(0).setSize(1000);
        SearchResponse response = getSearchResponse(client, searchParams);
        SearchHits hits = response.getHits();
        for (SearchHit hit : hits) {
            Map<String, Object> source = hit.getSourceAsMap();
            log.debug("writing ... {}", source);
            try {
                String data = new ObjectMapper().writeValueAsString(source);
                writeRes2Json(data, path);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        long totalHits = hits.getTotalHits();
        log.info("|===> Hits {}", totalHits);
        return totalHits;
    }

    public long test11(String date, String name) {
        BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("complaint_dt", date));
        if (name != null) {
            queryBuilder.must(QueryBuilders.termQuery("family_name", name));
        }
        SearchParams searchParams = new SearchParams()
                .setIndex("ic")
                .setType("T_KEYWORD_EXTRACT")
                .setQueryBuilder(queryBuilder)
                .setIncludes("_", "sdafasdfasdf")// 不影响
                .setOrderField("timestamp123123")// 影响
                .setOrder(SortOrder.DESC)
                .setFrom(0).setSize(1000);
        SearchResponse response = getSearchResponse(client, searchParams);
        SearchHits hits = response.getHits();
        // for (SearchHit hit : hits) {
        //     Map<String, Object> source = hit.getSourceAsMap();
        //     log.debug("writing ... {}", source);
        //     try {
        //         String data = new ObjectMapper().writeValueAsString(source);
        //         writeRes2Json(data, path);
        //     } catch (JsonProcessingException e) {
        //         e.printStackTrace();
        //     }
        // }
        long totalHits = hits.getTotalHits();
        log.info("|===> Hits {}", totalHits);
        return totalHits;
    }
}

