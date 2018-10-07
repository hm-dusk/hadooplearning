package top.cyanide.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.DecimalFormat;

/**
 * @author liming
 * @date Created in 2018/10/7 18:15
 */
public class HbaseTest {
	private Connection conn;

	@Before
	public void before() throws IOException {
		//创建Hbase配置对象(使用的就是hadoop configuration)
		Configuration conf = HBaseConfiguration.create();
		//创建连接对象
		conn = ConnectionFactory.createConnection(conf);
	}

	/**
	 * 插入
	 */
	@Test
	public void testPut() throws Exception {
		//得到表对象
		TableName tname = TableName.valueOf("ns1:t1");
		Table t = conn.getTable(tname);
		byte[] rowkey = Bytes.toBytes("row2");
		Put put = new Put(rowkey);
		//row2/f1:id/xx= 2
		put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("id"),Bytes.toBytes(2));
		//row2/f1:name/xx= 'tomas'
		put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes("tomas"));
		//row2/f1:age/xx=13
		put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes(13));
		t.put(put);
		conn.close();
		System.out.println("ok");
	}

	/**
	 * 查询
	 */
	@Test
	public void testGet() throws Exception {
		//得到表对象
		TableName tname = TableName.valueOf("ns1:t1");
		Table t = conn.getTable(tname);
		Get get = new Get(Bytes.toBytes("row2"));
		//get.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"));
		Result r = t.get(get);
		byte[] nameBytes = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name"));
		byte[] idBytes = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("id"));
		byte[] ageBytes = r.getValue(Bytes.toBytes("f1"), Bytes.toBytes("age"));
		conn.close();
		System.out.println(Bytes.toString(nameBytes));
		System.out.println(Bytes.toInt(idBytes));
		System.out.println(Bytes.toInt(ageBytes));
	}

	/**
	 * 批量插入数据
	 */
	@Test
	public void batchPut() throws IOException {
		TableName tableName = TableName.valueOf("ns1:t1");
		//数字格式化
		DecimalFormat df = new DecimalFormat("000000");
		HTable t = (HTable) conn.getTable(tableName);
		//关闭自动刷新
		t.setAutoFlushTo(false);
		for (int i = 0; i < 1000; i++) {
			Put put = new Put(Bytes.toBytes("row"+df.format(i)));
			//关闭写前日志
			put.setWriteToWAL(false);
			put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("id"),Bytes.toBytes(i));
			put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes("zhangsan"+df.format(i)));
			put.addColumn(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes(i%100));
			t.put(put);

			//手动刷新
			if (i%200 ==0 ){
				t.flushCommits();
			}
		}
		t.flushCommits();
		t.close();
		conn.close();
	}

}
