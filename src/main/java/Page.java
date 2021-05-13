import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.Vector;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class Page implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private int numberOfElements;
	private int maximumSize;
	private String path, clusteringKeyColumn, clusteringKeyType;
	private Vector<Tuple> tuples; // changed to vector as required

	/**
	 * constructor of the page
	 * 
	 * @param maximumSize            maximum number of tuples allowed in this page
	 * @param path                   path to the page file on disk
	 * @param strClusteringKeyColumn the column name used as a clustering key
	 * @param strClusteringKeyType   the data type of the clustering key
	 */
	public Page(int maximumSize, String path, String strClusteringKeyColumn, String strClusteringKeyType) {

		this.maximumSize = maximumSize;
		this.path = path;
		this.clusteringKeyColumn = strClusteringKeyColumn;
		this.clusteringKeyType = strClusteringKeyType;
		tuples = new Vector<Tuple>();
	}

	/**
	 * inserts the tuple in the specified location in page
	 * 
	 * @param htblColNameValue key-value pairs representing the tuple to be inserted
	 * @param insIdx           the position the tuple to be inserted in
	 * @return the kicked out tuple if it exists
	 * @throws FileNotFoundException when saving fails
	 * @throws IOException           when saving fails
	 */
	public Tuple insert(Hashtable<String, Object> htblColNameValue, int insIdx)
			throws FileNotFoundException, IOException {

		// saves the state of the page before insert
		boolean wasFull = isFull();

		// populate a new tuple with insert values
		Tuple tuple = new Tuple(clusteringKeyColumn);
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			tuple.setValue(e.getKey(), e.getValue());
		}

		// validates the insert position and adds the tuple in position
		if (insIdx < tuples.size()) {
			tuples.add(insIdx, tuple);
		} else {
			tuples.add(tuple);
		}

		// depending on the saved state either a tuple is kicked out or nothing changes
		if (wasFull) {
			Tuple outTuple = tuples.remove(numberOfElements);
			save();
			return outTuple;
		} else {
			numberOfElements++;
			save();
			return null;
		}
	}

	/**
	 * inserts the tuple at the end of the page
	 * 
	 * @param htblColNameValue key-value pairs representing the tuple to be inserted
	 * @throws FileNotFoundException when saving fails
	 * @throws IOException           when saving fails
	 */
	public void insert(Hashtable<String, Object> htblColNameValue) throws FileNotFoundException, IOException {
		// populate a new tuple with insert values
		Tuple tuple = new Tuple(clusteringKeyColumn);
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			tuple.setValue(e.getKey(), e.getValue());
		}

		// push new tuple to the end of existing vector
		tuples.add(tuple);

		// increment size of page and save
		numberOfElements++;
		save();
	}

	/**
	 * binary searches to find the location the value passed should be in
	 * 
	 * @param value clustering key value to be inserted
	 * @return position of insertion in page
	 * @throws DBAppException
	 */
	public int getInsertIdx(Object value) throws DBAppException {
		// gets value as comparable
		Comparable valueC = getComparable(value, clusteringKeyType);

		// binary search to find value's location which is guaranteed to be found
		int lo = 0, hi = numberOfElements - 1, res = -1;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable oValueC = getComparable(tuples.get(mid).getClusteringKeyValue(), clusteringKeyType);
			if (valueC.compareTo(oValueC) <= 0) {
				res = mid;
				hi = mid - 1;
			} else {
				lo = mid + 1;
			}
		}

		Comparable oValueC = getComparable(tuples.get(res).getClusteringKeyValue(), clusteringKeyType);
		if (valueC.compareTo(oValueC) == 0) {
			throw new DBAppException("Couldn't insert into table as a tuple with the clustering key `" + value
					+ "` already exists in table.");
		}

		// returns position of insert
		return res;
	}

	/**
	 * binary searches to find tuple with specified clustering key value and updates
	 * it
	 * 
	 * @param clusteringKeyValue value of the clustering key in the tuple to be
	 *                           updated
	 * @param colNameValue       key-value pairs representing the column names and
	 *                           their new values
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public Tuple update(Comparable clusteringKeyValue, Hashtable<String, Object> colNameValue)
			throws FileNotFoundException, IOException {
		// binary search using clustering key provided to find the tuple to update
		int lo = 0, hi = numberOfElements - 1, res = -1;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable oValueC = getComparable(tuples.get(mid).getClusteringKeyValue(), clusteringKeyType);
			int chk = clusteringKeyValue.compareTo(oValueC);
			if (chk == 0) {
				res = mid;
				break;
			} else if (chk > 0) {
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		// if tuple index is found then update the tuple at this position
		if (res != -1) {
			return update(res, colNameValue);
		}
		return null;
	}

	/**
	 * updates the tuple at the specified location on page
	 * 
	 * @param index            the location of the tuple to be updated
	 * @param htblColNameValue the column names to be updated and their values
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private Tuple update(int index, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		// get the tuple at the specified index
		Tuple t = tuples.get(index);

		// set the tuple values to the new values
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			t.setValue(e.getKey(), e.getValue());
		}

		// save the page
		save();

		return t;
	}

	/**
	 * 
	 * @param htblColNameValue
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void delete(Hashtable<String, Object> htblColNameValue) throws FileNotFoundException, IOException {
		boolean changed = false;
		// linear search over the tuples in the page
		for (int i = 0; i < numberOfElements; ++i) {
			// check if tuple matches all values in the criteria
			boolean flag = true;
			for (Entry<String, Object> e : htblColNameValue.entrySet()) {
				flag &= tuples.get(i).checkKeyValue(e.getKey(), e.getValue());
			}

			// if it matches then delete it
			if (flag) {
				changed = true;
				tuples.remove(i--);
				numberOfElements--;
			}
		}

		// if deletion happened then save page
		if (changed) {
			save();
		}
	}

	/**
	 * deletes the tuples in page with the specified clustering key value
	 * 
	 * @param clusteringKeyValue the value of the clustering key in the tuple to be
	 *                           deleted
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public void delete(Object clusteringKeyValue, Hashtable<String, Object> htblColNameValue)
			throws FileNotFoundException, IOException {
		// binary search using clustering key value to find tuple position
		int lo = 0, hi = numberOfElements - 1, res = -1;
		Comparable valueC = getComparable(clusteringKeyValue, clusteringKeyType);
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable oValueC = getComparable(tuples.get(mid).getClusteringKeyValue(), clusteringKeyType);
			int chk = valueC.compareTo(oValueC);
			if (chk == 0) {
				res = mid;
				break;
			} else if (chk > 0) {
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}

		// if tuple exists and position found then delete tuple at this position
		if (res != -1) {
			boolean flag = true;

			for (Entry<String, Object> e : htblColNameValue.entrySet()) {
				if (tuples.get(res).getValues().get(e.getKey()) == null) {
					flag = false;
				} else {
					flag &= tuples.get(res).checkKeyValue(e.getKey(), e.getValue());
				}
			}

			if (flag) {
				tuples.remove(res);
				numberOfElements--;
				save();
			}
		}
	}

	/*
	 * HELPER METHODS
	 */

	private Comparable getComparable(Object o, String type) {
		Comparable res = null;
		switch (type) {
		case "java.lang.Integer":
			res = (Integer) o;
			break;
		case "java.lang.String":
			res = (String) o;
			break;
		case "java.lang.Double":
			res = (Double) o;
			break;
		case "java.util.Date":
			res = (Date) o;
			break;
		default:
			break;
		}
		return res;
	}

	private void save() throws FileNotFoundException, IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path));
		oos.writeObject(this);
		oos.close();
	}

	public boolean tupleExists(Comparable clusteringKeyValue) {
		int lo = 0, hi = numberOfElements - 1, res = -1;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable oValueC = getComparable(tuples.get(mid).getClusteringKeyValue(), clusteringKeyType);
			int chk = clusteringKeyValue.compareTo(oValueC);
			if (chk == 0) {
				res = mid;
				break;
			} else if (chk > 0) {
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		return res != -1;
	}

	public Tuple getTupleAt(int idx) {
		return tuples.get(idx);
	}

	public Tuple getTuple(Comparable clusteringKeyValue) {
		// binary search using clustering key provided to find the tuple to update
		int lo = 0, hi = numberOfElements - 1, res = -1;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable oValueC = getComparable(tuples.get(mid).getClusteringKeyValue(), clusteringKeyType);
			int chk = clusteringKeyValue.compareTo(oValueC);
			if (chk == 0) {
				res = mid;
				break;
			} else if (chk > 0) {
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		// if tuple index then return this tuple
		return res != -1 ? tuples.get(res) : null;
	}
	
	public int getIndexOf(Comparable clusteringKeyValue) {
		int lo = 0, hi = numberOfElements - 1, res = -1;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable oValueC = getComparable(tuples.get(mid).getClusteringKeyValue(), clusteringKeyType);
			int chk = clusteringKeyValue.compareTo(oValueC);
			if (chk == 0) {
				res = mid;
				break;
			} else if (chk > 0) {
				lo = mid + 1;
			} else {
				hi = mid - 1;
			}
		}
		// if tuple index then return index of this tuple
		return res;
	}

	public Tuple getLast() {
		return getTupleAt(tuples.size() - 1);
	}

	public Vector<Tuple> getTuples() {
		return tuples;
	}

	public int getSize() {
		return numberOfElements;
	}

	public boolean isFull() {
		return numberOfElements == maximumSize;
	}

	public boolean isEmpty() {
		return numberOfElements == 0;
	}

	public String toString() {
		StringBuilder res = new StringBuilder("########################### PAGE  ###########################\n");
		int i = 0;
		for (Tuple t : tuples) {
			res.append((i++) + ":").append(t.toString()).append("\n");
		}
		return res.toString();
	}
}
