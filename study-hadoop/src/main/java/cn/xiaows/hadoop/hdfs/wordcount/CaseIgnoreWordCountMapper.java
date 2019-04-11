package cn.xiaows.hadoop.hdfs.wordcount;

public class CaseIgnoreWordCountMapper implements Mapper {
	@Override
	public void map(String line, Context context) {
		String[] words = line.toLowerCase().split(" ");
		for (String word : words) {
			Object value = context.get(word);
			if (null == value) {
				context.write(word, 1);
			} else {
				context.write(word, (int) value + 1);
			}
		}
	}
}
