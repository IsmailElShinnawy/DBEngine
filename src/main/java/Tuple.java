import java.io.Serializable;
import java.util.Hashtable;

public class Tuple implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Hashtable<String, Object> values;
	private String clusteringKeyColumn;

	/**
	 * constructor of a tuple
	 * 
	 * @param strClusteringKeyColumn the clustering key column name
	 */
	public Tuple(String strClusteringKeyColumn) {
		values = new Hashtable<String, Object>();
		this.clusteringKeyColumn = strClusteringKeyColumn;
	}

	/**
	 * sets the key of the tuple to the specified value
	 * 
	 * @param key   the key to be set
	 * @param value the value to be inserted
	 */
	public void setValue(String key, Object value) {
		values.put(key, value);
	}

	/**
	 * checks if the tuple's key value matches the passed value
	 * 
	 * @param key   the key to compare
	 * @param value the value to be checked against
	 * @return true if the tuple value matches the passed value, false otherwise
	 */
	public boolean checkKeyValue(String key, Object value) {
		if (values.get(key) == null)
			return false;
		return values.get(key).equals(value);
	}

	/**
	 * @return key-value pairs inside the tuple
	 */
	public Hashtable<String, Object> getValues() {
		return values;
	}

	/**
	 * @return the clustering key value in this pair
	 */
	public Object getClusteringKeyValue() {
		return values.get(clusteringKeyColumn);
	}

	public Object getValue(String key) {
		return values.get(key);
	}

	public String toString() {
		return values.toString();
	}
}
