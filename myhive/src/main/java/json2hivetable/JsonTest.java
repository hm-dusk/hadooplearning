package json2hivetable;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;
import java.util.Set;

/**
 * @author liming
 * @date Created in 2018/9/29 9:19
 */
public class JsonTest {
	private Logger logger = LoggerFactory.getLogger(this.getClass());

	@Test
	public void test1() throws IOException {
		List<JsonMetaNode> jsonMetaNodeList = Lists.newArrayList();
		// 读取文件
		File file = new File("D://json.txt");
		FileReader fileReader = null;
		try {
			fileReader = new FileReader(file);
		} catch (FileNotFoundException e) {
			logger.error("文件不存在",e.getMessage(),e);
		}
		assert fileReader != null;
		BufferedReader bufferedReader = new BufferedReader(fileReader);

		String s;
		StringBuffer stringBuffer = new StringBuffer();
		// 讲文件所有的内存读取出来
		while ((s = bufferedReader.readLine()) != null) {

			stringBuffer.append(s);

		}
		// 转换成字符串
		if (stringBuffer != null) {
			s = stringBuffer.toString();
		}
		// 转换成json对象
		JSONObject jsonObject = (JSONObject) JSON.parse(s);

		Set<String> strings = jsonObject.keySet();

		// 遍历json对象，根据key获取value并获取value的类型
		for (String string : strings) {
			JsonMetaNode jsonMete = new JsonMetaNode();
			jsonMete.setKey(string);
			Object o = jsonObject.get(string);
			String name = o.getClass().getName();
			//json值为json类，则进行子节点的挂载
			if (name.equals(SqlSentence.JSONOBJECT)){
				JsonMetaNode jsonMetaNode = new JsonMetaNode();
				jsonMetaNode.setKey(string);

				//将该节点添加进父节点的子节点list中
				jsonMete.setChildren(Lists.newArrayList(jsonMetaNode));
			}
			jsonMete.setValueType(name);
			jsonMetaNodeList.add(jsonMete);

		}
		//　调用建表语句的方法
		String sqlCreateTable = SqlUtil.createTable("sql_test", jsonMetaNodeList);
		System.out.println(sqlCreateTable);
	}

	private JsonMetaNode parseJsonNode(JSONObject jsonObject, JsonMetaNode jsonMetaNode){
//		jsonObject.ma

		return jsonMetaNode;
	}
}
