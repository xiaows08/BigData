package cn.xiaows.studyelasticsearch5.conf;

import org.springframework.beans.BeanUtils;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.util.Arrays;

@Configuration
public class Config {

    @Bean
    @PostConstruct
    public Properties instantiate() {
        return BeanUtils.instantiateClass(Properties.class);
    }

    @Bean
    public TransportClientFactory transportClientFactory() {
        System.out.println("==============================================");
        System.out.println("================= ESConfig ... ===============");
        System.out.println("==============================================");
        System.out.println(Arrays.asList(Properties.ES_CLUSTER_HOSTS));
        TransportClientFactory transportClientFactory = new TransportClientFactory();
        transportClientFactory.setClusterName(Properties.ES_CLUSTER_NAME);
        transportClientFactory.setClusterHosts(Properties.ES_CLUSTER_HOSTS);
        return transportClientFactory;
    }
}
