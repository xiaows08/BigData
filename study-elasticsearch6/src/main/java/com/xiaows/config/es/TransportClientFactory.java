package com.xiaows.config.es;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import java.net.InetAddress;

public class TransportClientFactory implements FactoryBean<TransportClient>, InitializingBean, DisposableBean {
	private String clusterName;
	private String[] clusterHosts;
	private TransportClient client;

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public void setClusterHosts(String[] clusterHosts) {
		this.clusterHosts = clusterHosts;
	}

	@Override
	public void destroy() throws Exception {
		if (client != null) {
			client.close();
		}
	}

	@Override
	public TransportClient getObject() throws Exception {
		return client;
	}

	@Override
	public Class<?> getObjectType() {
		return TransportClient.class;
	}

	@Override
	public void afterPropertiesSet() throws Exception {
		Settings settings = Settings.builder()
				.put("cluster.name", this.clusterName)
				.put("client.transport.ignore_cluster_name", true)
				.put("client.transport.sniff", true)
				.build();

		client = new PreBuiltTransportClient(settings);
		for (String clusterHost : this.clusterHosts) {
			String[] cluster = clusterHost.split(":");
			client.addTransportAddress(new TransportAddress(InetAddress.getByName(cluster[0]), Integer.parseInt(cluster[1])));
		}

		System.out.println("**************  ESConfig Done **************");
	}

	@Override
	public boolean isSingleton() {
		return false;
	}
}
