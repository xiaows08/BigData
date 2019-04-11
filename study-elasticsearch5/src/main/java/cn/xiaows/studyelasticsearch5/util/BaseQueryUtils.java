package cn.xiaows.studyelasticsearch5.util;

import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.zip.GZIPInputStream;

@Slf4j
public class BaseQueryUtils {

    /**
     * Example:
     * <pre>
     *  client.prepareSearch(index)
     *        .setTypes(type)
     *        .setQuery(QueryBuilders.boolQuery()
     *        .must(QueryBuilders.matchQuery("data_source", "T_12365AUTO_COMPLAINT"))
     *        .must(QueryBuilders.rangeQuery("complaint_dt").from("now-13d/d"))
     *        .must(QueryBuilders.matchQuery("family_name", "荣威"))
     *        .must(QueryBuilders.matchQuery("sentiment_score", -1))//负面评论 )
     *        .setFetchSource(include, null)// 显示指定字段
     *        .addSort("complaint_dt", SortOrder.ASC)//按字段排序
     *        .addAggregation(AggregationBuilders.dateHistogram("complaint_dt").format("MM-dd").field("complaint_dt")
     *        .dateHistogramInterval(DateHistogramInterval.DAY) )//按日期分组
     *        .setFrom(0).setSize(1000);//分页
     * </pre>
     * @param sp{client, index, types, queryBuilder, aggregation, from, size, includes, excludes, sort}
     * @return
     */
    protected SearchResponse getSearchResponse(TransportClient client, SearchParams sp) {
        SearchRequestBuilder requestBuilder = null;
        try {
            requestBuilder = client.prepareSearch(sp.getIndex()).setTypes(sp.getType())
                    .setQuery(sp.getQueryBuilder()).setFetchSource(sp.getIncludes(), sp.getExcludes())
                    .setFrom(sp.getFrom()).setSize(sp.getSize());
            if (sp.getOrderField().indexOf(",") != -1) {
                for (String orderField : sp.getOrderField().split(",")) {
                    if (orderField.equals("complaint_dt")) {
                        requestBuilder.addSort(orderField, SortOrder.DESC);
                    } else if (orderField.equals("priority")) {
                        requestBuilder.addSort(orderField, SortOrder.ASC);
                    } else {
                        requestBuilder.addSort(orderField, SortOrder.ASC);
                    }
                }
            } else {
                requestBuilder.addSort(sp.getOrderField(), sp.getOrder());
            }

            if (sp.getAggregationBuilder() != null) {
                requestBuilder.addAggregation(sp.getAggregationBuilder());
            }
            return requestBuilder.get();
        } catch (Exception e) {
            log.error("Search has exception --> {}", e);
        }
        return null;
    }

    protected void writeRes2Json(String data, String path) {
        try {
            File file = new File(path);
            log.debug("File_Path ==> {}", file.getPath());
            if (!file.exists()) {
                file.createNewFile();
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile(), true);
            PrintWriter pw = new PrintWriter(fw);
            pw.println(data);
            pw.flush();
            fw.flush();
            pw.close();
            fw.close();
            // BufferedWriter bw = new BufferedWriter(fw);
            // bw.append(data);
            // bw.close();
            // fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将content写入服务器的snapshot文件夹内,并以sourceId.html命名文件<br>
     * <p>如果用户之前访问过,后台之前生成了一份快照,再次访问是否重复写一次?影响性能!</p>
     *
     * @param sourceId
     * @param content
     */
    public void write2File(String sourceId, String content) {
        try {
            /**System.out.println(BaseDaoImpl.class.getResource("../../"));
             System.out.println(BaseDaoImpl.class.getClassLoader().getResource("").getFile());
             System.out.println(this.getClass().getClassLoader().getResource("../../").getPath());*/

            File file = new File(this.getClass().getClassLoader().getResource("../../").getPath() + "snapshot/" + sourceId + ".html");
            log.debug("File_Path ==> {}", file.getPath());
            if (!file.exists()) {
                log.info("Create File [{}.html]... \t", sourceId);
                file.createNewFile();
            } else {
                log.info("File: [{}.html] already exists ...", sourceId);
                return;
            }
            FileWriter fw = new FileWriter(file.getAbsoluteFile());
            BufferedWriter bw = new BufferedWriter(fw);
            bw.write(content);
            bw.close();
			
			/*
			BufferedWriter out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(
					this.getClass().getClassLoader().getResource("../../").getPath() + "snapshot/" + sourceId + ".html"), "UTF-8"));  
            out.write(content);  
            out.newLine();
            out.flush();
            out.close();
             */
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 字符串转化为byte
     *
     * @param msg
     * @return
     * @throws IOException
     */
    public static byte[] uncompressByteArray(String msg) throws IOException {
        byte[] compressed = msg.getBytes("ISO8859_1");
        return uncompress(compressed);
    }

    /**
     * 解压缩
     *
     * @param data
     * @return
     * @throws IOException
     */
    public static byte[] uncompress(byte[] data) throws IOException {
        if (data == null || data.length == 0) {
            return data;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(data);
        GZIPInputStream gunzip = new GZIPInputStream(in);
        byte[] buffer = new byte[16];
        int n;
        while ((n = gunzip.read(buffer)) >= 0) {
            out.write(buffer, 0, n);
        }
        gunzip.close();
        in.close();
        return out.toByteArray();
    }

    /**
     * bytes转化为字符文件
     *
     * @param bytes    字节数组
     * @param filePath 目标路径
     * @param fileName 文件名
     * @return String fileName
     */
    public static String getFileByBytes(byte[] bytes, String filePath, String fileName) {
        BufferedOutputStream bos = null;
        FileOutputStream fos = null;
        File file = null;
        try {
            File dir = new File(filePath);
            if (!dir.exists()) {// 判断文件目录是否存在
                dir.mkdirs();
            }
            file = new File(filePath + fileName);
            fos = new FileOutputStream(file);
            bos = new BufferedOutputStream(fos);
            bos.write(bytes);
//			return file.getAbsolutePath();
            System.out.println("---- Downloaded done -> " + fileName);
            return file.getName();
        } catch (Exception e) {
            System.out.println("#### snapshot download field!!! " + file.getName());
            e.printStackTrace();
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (fos != null) {
                try {
                    fos.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return null;
    }
}
