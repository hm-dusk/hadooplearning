package top.cyanide.myhive;

import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;

/**
 * @author liming
 * @date Created in 2018/8/18 17:36
 */
public class TestHive {
	private Connection con;

	@Before
	public void init() throws Exception {
		String driverClass = "org.apache.hive.jdbc.HiveDriver";
		Class.forName(driverClass);
//		String url = "jdbc:hive2://192.168.40.3:10000/mydb";
		String url = "jdbc:hive2://10.75.4.100:10000/testdb";
		con = DriverManager.getConnection(url);
	}

	@Test
	public void select() throws Exception {
//		System.out.println(con);
		Statement st = con.createStatement();
		ResultSet rs = st.executeQuery("SELECT * FROM mytable");
		while (rs.next()) {
			int id = rs.getInt("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println(id + ", " + name + ", " + age);
		}
	}

	@Test
	public void insert() throws Exception {
		Statement st = con.createStatement();
		for (int i = 0; i < 10; i++) {
			st.executeUpdate("INSERT INTO test VALUES (" + new Random().nextInt(10000) +") ");
		}
//		st.addBatch("INSERT INTO test VALUES (" + new Random().nextInt(10000) +") ;" +"INSERT INTO test VALUES (" + new Random().nextInt(10000) +") ;");
////		st.set
//		st.executeBatch();
	}
}
