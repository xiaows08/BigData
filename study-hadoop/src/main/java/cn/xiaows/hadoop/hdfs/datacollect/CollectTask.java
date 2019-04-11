package cn.xiaows.hadoop.hdfs.datacollect;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimerTask;
import java.util.UUID;

@Slf4j
public class CollectTask extends TimerTask {
	@Override
	public void run() {
		String today = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
		File srcDir = new File("/logs/access/");

		// 列出源目录中需要采集的文件(满足命名格式条件)
		File[] files = srcDir.listFiles(/*new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				// if (name.startsWith("role")) {
				// 	return true;
				// }
				// if (name.endsWith(".gz")) {
				// 	return true;
				// }
				return false;
			}
		}*/);
		log.info("探测到如下文件需要采集: {}", Arrays.toString(files));
		try {
			// 将采集的文件移动到待上传的临时目录
			File uploadDir = new File("/logs/toUpload/");
			for (File file : files) {
				log.info("copy ... {}", file.getName());
				// FileUtils.moveFileToDirectory(file, uploadDir, true);
				FileUtils.copyFileToDirectory(file, uploadDir, true);
				log.info("copy done {}", file.getName());
			}

			//构造一个HDFS的客户端对象
			FileSystem fs = FileSystem.get(new URI("hdfs://172.17.0.2:9000/"), new Configuration(), "root");

			// 检查HDFS中的目录是否存在,如果不存在,则创建
			Path hdfsDestPath = new Path("/package/");
			if (!fs.exists(hdfsDestPath)) {
				fs.mkdirs(hdfsDestPath);
			}

			File[] uploadFiles = uploadDir.listFiles();
			File backupDir = new File("/logs/backup/" + today + "/");
			for (File file : uploadFiles) {
				Path dstPath = new Path("/package/" + today + "/" + UUID.randomUUID() + file.getName());
				log.info("uploading... {}", file.getName());
				fs.copyFromLocalFile(new Path(file.getAbsolutePath()), dstPath);
				log.info("uploaded {}", dstPath.getName());

				log.info("backup... {}", file.getName());
				file.delete();
				// FileUtils.moveFileToDirectory(file, backupDir, true);
				log.info("backup done {}", file.getName());
			}
			for (int i = 0; i < 20; i++) {
				System.out.println("======================================");
			}
			Thread.sleep(10*1000L);
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
	}
}
