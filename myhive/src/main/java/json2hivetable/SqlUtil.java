package json2hivetable;

import java.util.List;

/**
 * @author liming
 * @date Created in 2018/9/29 9:27
 */
public class SqlUtil {
	/**
	 * 建表语句
	 *
	 * @param tableName        表名
	 * @param jsonMetaNodeList json节点链表list
	 * @return SQL语句
	 */
	public static String createTable(String tableName, List<JsonMetaNode> jsonMetaNodeList) {
		return "CREATE TABLE " + tableName + "(\n" + getRowName(jsonMetaNodeList);
	}

	/**
	 * 获取建表语句列名
	 *
	 * @param jsonMetaNodeList json节点链表list
	 * @return 列名SQL语句
	 */
	private static String getRowName(List<JsonMetaNode> jsonMetaNodeList) {

		StringBuffer sqlRowNameBuffer = new StringBuffer();


		for (JsonMetaNode jsonMetaNode : jsonMetaNodeList) {

			String key = jsonMetaNode.getKey();
			String valueType = jsonMetaNode.getValueType();
			String type = "";
			if (SqlSentence.INTEGER.equals(valueType)) {
				type = "int(100)";
			} else if (SqlSentence.LONG.equals(valueType)) {
				type = "bigint(100)";
			} else if (SqlSentence.STRING.equals(valueType)) {
				type = "varchar(100)";
			} else if (SqlSentence.BIG_DECIMAL.equals(valueType)) {
				type = "decimal(18,8)";
			} else if (SqlSentence.FLOAT.equals(valueType)) {
				type = "float(100,10)";
			} else if (SqlSentence.DOUBLE.equals(valueType)) {
				type = "double(100,10)";
			} else if (SqlSentence.DATE.equals(valueType)) {
				type = "date";
			} else if (SqlSentence.BOOLEAN.equals(valueType)) {
				type = "boolean";
			} else {
				type = "varchar(100)";
			}
			sqlRowNameBuffer.append(key).append(" ").append(type).append(" ").append("CHARACTER SET utf8 NULL ,");

		}
		sqlRowNameBuffer.deleteCharAt(sqlRowNameBuffer.length() - 1);
		sqlRowNameBuffer.append(")");
		return sqlRowNameBuffer.toString();
	}
}
