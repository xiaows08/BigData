package cn.xiaows.hadoop.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;

public class HdfsClientDemo {

	private FileSystem fs = null;

	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		// 指定本客户端上传文件到hdfs是需要保存的副本数
		conf.set("dfs.replication", "2");
		// 指定本客户端上传文件到hdfs是切块的数据大小为64M
		conf.set("dfs.blocksize", "64m");
		fs = FileSystem.get(new URI("hdfs://hadoop-slave0:9000"), conf, "root");
	}

	@Test
	public void testUpload() throws IOException {
		fs.copyFromLocalFile(new Path("E:/tmp/hadoop.log"), new Path("/hadoop.log"));
	}

	@Test
	public void testGet() throws IOException {
		fs.copyToLocalFile(new Path("/hadoop.log"), new Path("E:/"));
	}

	@Test
	public void testRename() throws IOException {
		fs.rename(new Path("/hadoop.log"), new Path("/logs-hadoop.log"));
		fs.close();
	}

	@Test
	public void testCat() throws IOException {
		FSDataInputStream in = fs.open(new Path("/logs-hadoop.log"));
		BufferedReader d = new BufferedReader(new InputStreamReader(in));
		String line;
		while ((line = d.readLine()) != null) {
			System.out.println(line);
		}
		System.out.println();
	}


	public static void main(String args[]) throws Exception {
		/**
		 * Configuration参数对象的机制
		 * 	构造时,会加载jar包中的配置文件
		 */
		// new Configuration()会从项目的classpath中加载core-default.xml hdfs-default.xml core-site.xml hdfs-site.xml
		Configuration conf = new Configuration();

		// 指定本客户端上传文件到hdfs是需要保存的副本数
		conf.set("dfs.replication", "2");
		// 指定本客户端上传文件到hdfs是切块的数据大小为64M
		conf.set("dfs.blocksize", "64m");
		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-slave0:9000"), conf, "root");

		fs.copyFromLocalFile(new Path("E:/Package/apache-ant-1.10.3-bin.zip"), new Path("/"));
		fs.close();
	}
}
