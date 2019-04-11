package com.xiaows.config.es;

import org.ini4j.Ini;
import org.ini4j.Profile;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.util.ResourceUtils;

import java.io.IOException;

@Configuration
public class ESConfig {

	@Bean
	public TransportClientFactory transportClientFactory() {
		System.out.println("**************  ESConfig ...  **************");
		Ini ini = null;
		try {
			ini = new Ini(ResourceUtils.getFile("classpath:config.ini"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		Profile.Section section = ini.get("bch");
		String clusterName = section.get("CLUSTER_NAME");
		String[] clusterHosts = section.get("CLUSTER_HOSTS").split(" ");

		TransportClientFactory transportClientFactory = new TransportClientFactory();
		transportClientFactory.setClusterName(clusterName);
		transportClientFactory.setClusterHosts(clusterHosts);
		return transportClientFactory;
	}
}

