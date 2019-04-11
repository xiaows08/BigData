package com.xiaows.dao;

import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;


/**
 * @Description:
 * @Auther: XIAOWS
 * @Date: 2018/7/26 14:39
 */
@Repository
public class ESUtils {
	@Autowired
	private TransportClient client;

	public XContentBuilder iterator(Map<String, Object> map) {
		try {
			XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
			for (Map.Entry<String, Object> entry : map.entrySet()) {
				builder.field(entry.getKey(), entry.getValue());
			}
			return builder.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	public IndexResponse insert(String index, String type, XContentBuilder builder) {
		return client.prepareIndex(index, type).setSource(builder).get();
	}

	public UpdateResponse update(String index, String type, String id, XContentBuilder builder) {
		try {
			return client.update(new UpdateRequest(index, type, id).doc(builder)).get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	public DeleteResponse delete(String index, String type, String id) {
		return client.prepareDelete(index, type, id).get();
	}

	public void test() {
		String index = "xiaows";
		String type = "t_xiaows";

		Map<String, Object> json = new HashMap<String, Object>();
		json.put("user", "kimchy");
		json.put("postDate", new Date());
		json.put("message", "trying out Elasticsearch");
		IndexRequestBuilder requestBuilder = client.prepareIndex(index, type).setSource(json);
		IndexResponse indexResponse = requestBuilder.get();
		System.out.println(indexResponse);
		try {
			IndexResponse response = client.prepareIndex(index, type).setSource(XContentFactory.jsonBuilder().startObject().field("user", "kimchy2").field("age", 22).field("postDate", new Date()).field("message", "trying out Elasticsearch").endObject()).get();
			System.out.println(response);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
