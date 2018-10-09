package json2hivetable;

import java.util.List;

/**
 * @author liming
 * @date Created in 2018/9/29 9:25
 */
public class JsonMetaNode {
	private String key;
	private String valueType;
	/**
	 * 数据库中的列名
	 */
	private String dbColName;
	private List<JsonMetaNode> children;


	public JsonMetaNode() {
	}

	public JsonMetaNode(String key, String valueType) {
		this.key = key;
		this.valueType = valueType;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValueType() {
		return valueType;
	}

	public void setValueType(String valueType) {
		this.valueType = valueType;
	}

	public String getDbColName() {
		return dbColName;
	}

	public void setDbColName(String dbColName) {
		this.dbColName = dbColName;
	}

	public List<JsonMetaNode> getChildren() {
		return children;
	}

	public void setChildren(List<JsonMetaNode> children) {
		this.children = children;
	}
}
