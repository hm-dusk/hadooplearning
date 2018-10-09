package top.cyanide.myhdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;

/**
 * @author liming
 * @date Created in 2018/8/16 9:34
 */
public class testHDFS {

	/**
	 * 读文件
	 */
	@Test
	public void textRead() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoopmaster:9000/");
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream in = fs.open(new Path("hdfs://hadoopmaster:9000/user/liming/test001.txt"));
		IOUtils.copyBytes(in, System.out, 1024);
		IOUtils.closeStream(in);
	}

	/**
	 * 写文件
	 */
	@Test
	public void testWrite() throws Exception {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoopmaster:9000/");
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream out = fs.create(new Path("hdfs://hadoopmaster:9000/user/liming/test001.txt"), true);
		ByteArrayInputStream in = new ByteArrayInputStream("i can do it".getBytes());
		IOUtils.copyBytes(in, out, 1024);
		IOUtils.closeStream(out);
	}
}
