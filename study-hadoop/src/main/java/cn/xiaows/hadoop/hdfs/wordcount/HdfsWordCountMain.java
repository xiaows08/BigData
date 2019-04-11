package cn.xiaows.hadoop.hdfs.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsWordCountMain {

	public static void main(String[] args) throws Exception {

		Properties props = new Properties();
		props.load(HdfsWordCountMain.class.getClassLoader().getResourceAsStream("job.properties"));

		Path input = new Path(props.getProperty("INPUT_PATH"));
		Path output = new Path(props.getProperty("OUTPUT_PATH"));

		Class<?> mapperClass = Class.forName(props.getProperty("MAPPER_CLASS"));
		Mapper mapper = (Mapper) mapperClass.newInstance();

		Context context = new Context();

		FileSystem fs = FileSystem.get(new URI("hdfs://hadoop-slave0:9000/"), new Configuration(), "root");
		RemoteIterator<LocatedFileStatus> iter = fs.listFiles(input, false);
		while (iter.hasNext()) {
			LocatedFileStatus file = iter.next();
			FSDataInputStream in = fs.open(file.getPath());
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			// 1.去hdfs中读文件:一次读一行
			String line = null;
			while ((line = reader.readLine()) != null) {
				// 2.调用一个方法对每一行进行业务处理
				mapper.map(line, context);
			}
			reader.close();
			in.close();
		}

		// 输出结果
		HashMap<Object, Object> contextMap = context.getContextMap();

		if (fs.exists(output)) {
			throw new RuntimeException("目录已存在, 请指定新目录!");
		}
		// if (!fs.exists(output)) {
		// 	fs.create(output);
		// }
		FSDataOutputStream out = fs.create(new Path(output, new Path("result.dat")));
		Set<Map.Entry<Object, Object>> entrySet = contextMap.entrySet();
		for (Map.Entry<Object, Object> entry : entrySet) {
			out.write((entry.getKey() + "\t" + entry.getValue() + "\n").getBytes());
		}
		out.close();
		fs.close();
		System.out.println("恭喜! 数据统计完成...");
		// 3.将这一行的处理结果放入缓存

		// 4.调用一个方法将缓存中的结果数据输出到hdfs结果文件


	}
}
