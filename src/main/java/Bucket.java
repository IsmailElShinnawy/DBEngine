import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

public class Bucket implements Serializable {

	class Pair implements Serializable {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String pageName;
		private int rowNumber;

		public Pair(String pageName, int rowNumber) {
			this.pageName = pageName;
			this.rowNumber = rowNumber;
		}

		public String getPageName() {
			return pageName;
		}

		public int getRowNumber() {
			return rowNumber;
		}

		public String toString() {
			return this.pageName + " " + this.rowNumber;
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Vector<Pair> refs;
	private int maxSize;
	private String tableDir, path;

	public Bucket(String path, int indexId, int bucketId, int maxSize) throws IOException {
		this.tableDir = path;
		this.path = path + "index_" + indexId + "_bucket_" + bucketId + ".class";
		this.maxSize = maxSize;
		this.refs = new Vector<Pair>();
	}

	public void insert(String pageName, int row) throws IOException {
		refs.add(new Pair(pageName, row));
		save();
	}

	public boolean remove(Hashtable<String, Object> htblColNameValue) throws ClassNotFoundException, IOException {
		for (int i = 0; i < refs.size(); ++i) {
			Pair p = refs.get(i);
			Page page = loadPage(p.getPageName());
			Tuple t = page.getTupleAt(p.getRowNumber());
			boolean flag = true;
			for (Entry<String, Object> e : t.getValues().entrySet()) {
				flag &= t.checkKeyValue(e.getKey(), htblColNameValue.get(e.getKey()));
			}
			for (Entry<String, Object> e : htblColNameValue.entrySet()) {
				flag &= t.checkKeyValue(e.getKey(), e.getValue());
			}
			if (flag) {
				refs.remove(i);
				save();
				return true;
			}
		}
		return false;
	}

	public void increment(String pageName, int idx, String ofPage, int maxPageSize) throws DBAppException, IOException {
		for (Pair p : refs) {
			if (p.pageName.equals(pageName) && p.rowNumber >= idx) {
				p.rowNumber++;
				if (p.rowNumber == maxPageSize) {
					if (ofPage == null) {
						// just for debugging purposes
						throw new DBAppException("Internal engine error, overflow set to null, but needed");
					}
					p.rowNumber = 0;
					p.pageName = ofPage;
				}
			}
		}
		save();
	}

	public void delete(TreeMap<String, LinkedList<Integer>> deletedPageNameRows) throws IOException {
		boolean save = false;
		for (int i = 0; i < refs.size(); ++i) {
			Pair p = refs.get(i);
			for (Entry<String, LinkedList<Integer>> e : deletedPageNameRows.entrySet()) {
				if (p.pageName.equals(e.getKey())) {
					save = true;
					if (e.getValue().contains(p.rowNumber)) {
						refs.remove(i);
						--i;
					} else {
						int count = 0;
						for (Integer deletedRow : e.getValue()) {
							if (p.rowNumber > deletedRow)
								count++;
						}
						p.rowNumber -= count;
					}
					break;
				}
			}
		}
		if (save)
			save();
	}

	public boolean isFull() {
		return refs.size() == maxSize;
	}

	public boolean isEmpty() {
		return refs.size() == 0;
	}

	public String getPath() {
		return path;
	}

	public Vector<Pair> getRefs() {
		return refs;
	}

	public void save() throws IOException {
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path));
		oos.writeObject(this);
		oos.close();
	}

	private Page loadPage(String pageName) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(tableDir + pageName));
		Page page = (Page) ois.readObject();
		ois.close();
		return page;
	}

	public String toString() {
		return refs.toString();
	}
}
