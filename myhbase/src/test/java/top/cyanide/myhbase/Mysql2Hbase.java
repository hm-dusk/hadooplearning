package top.cyanide.myhbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 测试QTouch mysql表转到HBase
 * @author liming
 * @date Created in 2018/10/9 20:37
 */
public class Mysql2Hbase {
	private static String PARTS_COMMON = "qtouch:partscommon";
	private static String BASEFAMILY = "baseinfo";
	private Connection hbaseCon;

	@Before
	public void before() throws IOException {
		//创建Hbase配置对象(使用的就是hadoop configuration)
		Configuration conf = HBaseConfiguration.create();
		//创建连接对象
		hbaseCon = ConnectionFactory.createConnection(conf);
	}

	private com.mysql.jdbc.Connection getConn() {
		String driver = "com.mysql.jdbc.Driver";
		String url = "jdbc:mysql://localhost:3306/qtouch1";
		String username = "root";
		String password = "1234";
		com.mysql.jdbc.Connection conn = null;
		try {
			Class.forName(driver); //classLoader,加载对应驱动
			conn = (com.mysql.jdbc.Connection) DriverManager.getConnection(url, username, password);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}

	@Test
	public void selectMysql() {
		com.mysql.jdbc.Connection con = this.getConn();
		String sql = "select * from t_partscommon limit 1,2";
		PreparedStatement ps;
		try {
			ps = con.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			//得到表对象
			TableName tname = TableName.valueOf(PARTS_COMMON);
			HTable t = (HTable) hbaseCon.getTable(tname);
			//关闭自动刷新
			t.setAutoFlushTo(false);
			while (rs.next()) {
				String id = rs.getString("id");
				Put put = new Put(Bytes.toBytes(id));
				//关闭写前日志
				put.setWriteToWAL(false);
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("parts_name"), Bytes.toBytes(rs.getString("parts_name")));
				if (rs.getString("company_id") != null) {
					put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("company_id"), Bytes.toBytes(rs.getString("company_id")));
				}
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("company_name"), Bytes.toBytes(rs.getString("company_name")));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("parts_type"), Bytes.toBytes(rs.getString("parts_type")));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("sendDate"), Bytes.toBytes(String.valueOf(rs.getLong("sendDate"))));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("startDate"), Bytes.toBytes(String.valueOf(rs.getLong("startDate"))));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("endDate"), Bytes.toBytes(String.valueOf(rs.getLong("endDate"))));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("keepTime"), Bytes.toBytes(String.valueOf(rs.getLong("keepTime"))));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("warnStatus"), Bytes.toBytes(rs.getString("warnStatus")));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("warnExp"), Bytes.toBytes(rs.getString("warnExp")));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("errorStatus"), Bytes.toBytes(rs.getString("errorStatus")));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("warnStatus"), Bytes.toBytes(rs.getString("warnStatus")));
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("errorExp"), Bytes.toBytes(rs.getString("errorExp")));
				if (rs.getDate("createTime") != null) {
					put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("createTime"), Bytes.toBytes(rs.getDate("createTime").toString()));
				}
				put.addColumn(Bytes.toBytes(BASEFAMILY), Bytes.toBytes("updateTime"), Bytes.toBytes(rs.getDate("updateTime").toString()));

				for (int i = 1; i < 16; i++) {
					if (rs.getString("column" + i) != null) {
						put.addColumn(Bytes.toBytes("otherinfo"), Bytes.toBytes("column" + i), Bytes.toBytes(rs.getString("column" + i)));
					}
				}

				t.put(put);
				System.out.println("ok");
			}
			t.flushCommits();

			try {
				t.close();
				hbaseCon.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (SQLException | IOException e) {
			e.printStackTrace();
		}
	}

	public Put createPut(String rowKey, String columnFamily) {
		byte[] key = Bytes.toBytes(rowKey);
		Put put = new Put(key);
		//row2/f1:id/xx= 2
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("id"), Bytes.toBytes(2));
		//row2/f1:name/xx= 'tomas'
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes("tomas"));
		//row2/f1:age/xx=13
		put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age"), Bytes.toBytes(13));
		return put;
	}

	/**
	 * 插入
	 */
//	@Test
	public void hbasePut(String tableName, Put put) throws Exception {
		//得到表对象
		TableName tname = TableName.valueOf(tableName);
		HTable t = (HTable) hbaseCon.getTable(tname);
		//关闭自动刷新
		t.setAutoFlushTo(false);
		t.put(put);
		hbaseCon.close();
		System.out.println("ok");
	}
}
