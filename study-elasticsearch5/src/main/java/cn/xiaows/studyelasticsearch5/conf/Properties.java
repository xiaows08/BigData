package cn.xiaows.studyelasticsearch5.conf;

import org.springframework.beans.factory.annotation.Value;

public class Properties {

    public static String ES_CLUSTER_NAME;
    public static String[] ES_CLUSTER_HOSTS;

    @Value("${customer.es.cluster.name}")
    public void setEsClusterName(String esClusterName) {
        ES_CLUSTER_NAME = esClusterName;
    }

    @Value("${customer.es.cluster.hosts}")
    public void setEsClusterHosts(String[] esClusterHosts) {
        ES_CLUSTER_HOSTS = esClusterHosts;
    }


    /** 这个留着 @PostConstruct 有坑 嘿嘿~ 自己体会~
     @Autowired private Environment env;
     public static Properties properties;
     @PostConstruct private void init() {
     this.ES_CLUSTER_NAME = env.getProperty("es.clusterName");
     this.ES_CLUSTER_HOSTS = env.getProperty("es.clusterHosts").split(",");
     this.INDEX_T_KEYWORD_EXTRACT = env.getProperty("es.index.t_keyword_extract");
     this.INDEX_T_PROB_AGG = env.getProperty("es.index.t_prob_agg");
     this.PAGES = env.getProperty("pages").split(",");
     }*/
}
