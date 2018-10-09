package top.cyanide.myzookeeper;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.junit.Test;

import java.util.List;

/**
 * @author liming
 * @date Created in 2018/8/16 15:57
 */
public class MyZooKeeper {
	/**
	 * 创建zk路径
	 */
	@Test
	public void createPath() throws Exception {
		ZooKeeper zk = new ZooKeeper("hadoopmaster:2181", 5000, null);
		/*
		路径
		数据
		ACL列表，控制权限
		节点类型，持久化节点
		 */
		String str = zk.create("/a/b", "tomas".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		System.out.println(str);
	}

	/**
	 * get路径
	 */
	@Test
	public void getPath() throws Exception {
		ZooKeeper zk = new ZooKeeper("hadoopmaster:2181", 5000, null);
		byte[] bytes = zk.getData("/a", null, null);
		System.out.println(new String(bytes));
	}

	/**
	 * 列出所有节点
	 */
	@Test
	public void listAllNode() throws Exception {
		String connect = "hadoopmaster:2181,hadoop001:2181,hadoop002:2181";
		ZooKeeper zk = new ZooKeeper(connect, 5000, null);
		listNode("/", zk);
	}

	private void listNode(String path, ZooKeeper zk) throws Exception {
		System.out.println(path);
		List<String> nodes = zk.getChildren(path, null, null);
		for (String node : nodes) {
			if (path.equals("/")) {
				listNode(path + node, zk);
			} else {
				listNode(path + "/" + node, zk);
			}
		}
	}
}
