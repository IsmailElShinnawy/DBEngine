import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.Vector;

@SuppressWarnings({ "rawtypes", "unchecked", "unused" })
public class Table implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String tableName, clusteringKeyColumn, clusteringKeyType, path;
	private int maxPageSize, maxBucketSize, nextPageIdx = 1, indexId = 1;
	private Vector<String> pages;
	private Vector<Object> maxKey;
	private Hashtable<String, String> htblColNameMin, htblColNameMax, htblColNameType;
	private Vector<GridIndex> indices;

	/*
	 * Constructor
	 */
	/**
	 * Constructor for the table
	 * 
	 * @param strTableName           the name of the table
	 * @param strClusteringKeyColumn the name of the column that would be used as a
	 *                               primary and clustering key
	 * @param htblColNameType        key-value pairs of column names and their data
	 *                               types
	 * @param strMainDir             path of the main directory of the DB engine to
	 *                               save resources
	 * @param intMaxPageSize         the maximum number of tuples allowed in a page
	 * @throws IOException when save is not successful
	 */
	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax, String strMainDir,
			int intMaxPageSize, int intMaxBucketSize) throws IOException {

		this.path = strMainDir + "data/" + strTableName + "/";
		this.tableName = strTableName;
		this.clusteringKeyColumn = strClusteringKeyColumn;
		this.clusteringKeyType = htblColNameType.get(strClusteringKeyColumn);
		this.maxPageSize = intMaxPageSize;
		this.maxBucketSize = intMaxBucketSize;

		this.htblColNameType = htblColNameType;
		this.htblColNameMax = htblColNameMax;
		this.htblColNameMin = htblColNameMin;

		this.pages = new Vector<String>();
		this.maxKey = new Vector<Object>();

		indices = new Vector();

		createDirectories();
		save();
	}

	/*
	 * Main methods
	 */
	/**
	 * Inserts the new tuple in its appropriate position in the table. Has support
	 * for overflow pages
	 * 
	 * @param htblColNameValue key-value pairs representing the tuple to be inserted
	 * @throws IOException            when save/load is not successful
	 * @throws ClassNotFoundException when load is not successful
	 * @throws DBAppException         if a tuple already exists with the clustering
	 *                                key wanted to be inserted
	 */
	public void insertWithOF(Hashtable<String, Object> htblColNameValue)
			throws IOException, ClassNotFoundException, DBAppException {
		if (pages.size() == 0) { // first insert so we create a new page and we insert in it blindly
			Page page = createPage();
			page.insert(htblColNameValue);

			// adds `page reference` at the end of the pages vector
			pages.add(tableName + "_" + nextPageIdx + ".class");

			// update indices: knowing the page name and that i inserted it in row 0 as this
			// is the first ever insert
			for (GridIndex gi : indices) {
				gi.insert(htblColNameValue, tableName + "_" + nextPageIdx + ".class", 0);
			}

			// updates the max key vector
			maxKey.add(htblColNameValue.get(clusteringKeyColumn));

			nextPageIdx++;

		} else { // if not first insert then we need to find correct insert page using binary
					// search
			// gets inserted key as comparable
			Comparable value = getComparable(htblColNameValue.get(clusteringKeyColumn), clusteringKeyType);

			// binary search using max key in each page
			int lo = 0, hi = pages.size() - 1, res = -1;
			while (lo <= hi) {
				int mid = lo + (hi - lo) / 2;
				// gets max key in page as comparable
				Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
				if (value.compareTo(max) <= 0) {
					res = mid;
					hi = mid - 1;
				} else {
					lo = mid + 1;
				}
			}

			if (res != -1) { // if a page with a greater key is found then it is the insert page
				// load target page and get the insert index within page
				Page page = getPage(res);
				int insIdx = page.getInsertIdx(htblColNameValue.get(clusteringKeyColumn));

				// inserts the tuple in its position within page and gets the kicked out tuple
				// if the page was already full
				Tuple outTuple = page.insert(htblColNameValue, insIdx);

				// updates the max key of the page by getting the last tuple in the sorted page
				maxKey.set(res, page.getLast().getClusteringKeyValue());

				String ofPage = null; // for updating the index

				if (outTuple != null) { // if there is a tuple which was kicked out of the full page
					if (res == pages.size() - 1) { // if last page was the page that kicked out the tuple then create a
													// new page to insert tuple

						// create a new page and insert blindly
						Page newPage = createPage();
						newPage.insert(outTuple.getValues());

						// updates the max key in the table
						maxKey.add(outTuple.getClusteringKeyValue());

						pages.add(tableName + "_" + nextPageIdx + ".class");

						ofPage = tableName + "_" + nextPageIdx + ".class";

						nextPageIdx++;
					} else { // if page in `middle` of table was the page that kicked the tuple
						// load in next page and check if it contains a free position
						Page nextPage = getPage(res + 1);

						if (!nextPage.isFull()) { // if next page contains a free space then insert at the beginning of
													// the page
							nextPage.insert(outTuple.getValues(), 0);

							ofPage = pages.get(res + 1);
						} else { // if next page is full then create an overflow page to insert the kicked out
									// tuple

							// create new page and insert in it blindly
							Page newPage = createPage();
							newPage.insert(outTuple.getValues());

							// update the max key table
							maxKey.add(res + 1, outTuple.getClusteringKeyValue());
							pages.add(res + 1, tableName + "_" + nextPageIdx + ".class");

							ofPage = tableName + "_" + nextPageIdx + ".class";

							nextPageIdx++;
						}
					}
				}

				// updates the indices
				for (GridIndex gi : indices) {
					// loop over all buckets in index and whenever matching page is found with row
					// >= insIdx then increment by one
					gi.increment(pages.get(res), insIdx, ofPage, maxPageSize);
					gi.insert(htblColNameValue, pages.get(res), insIdx);
				}
			} else { // if no page with greater key is found then insert in last page

				// load last page in table
				Page page = getPage(pages.size() - 1);
				if (!page.isFull()) { // if page has empty space then insert at its end and update maxKey
					page.insert(htblColNameValue);

					// update indices: knowing that the tuple is inserted in the last page and in
					// the last row
					for (GridIndex gi : indices) {
						gi.insert(htblColNameValue, pages.get(pages.size() - 1), page.getSize() - 1);
					}

					maxKey.set(maxKey.size() - 1, htblColNameValue.get(clusteringKeyColumn));
				} else { // if last page is full then create a new page at the end and insert tuple in it
							// blindly
					Page newPage = createPage();
					newPage.insert(htblColNameValue);
					pages.add(tableName + "_" + nextPageIdx + ".class");

					// update indices
					for (GridIndex gi : indices) {
						gi.insert(htblColNameValue, tableName + "_" + nextPageIdx + ".class", 0);
					}

					maxKey.add(htblColNameValue.get(clusteringKeyColumn));
					nextPageIdx++;
				}
			}
		}

		// saves the table to disk after every insert
		save();
	}

	/**
	 * updates the tuple with the passed clustering key. Supports binary search
	 * 
	 * @param clusteringKeyValue value of the clustering key of the tuple that needs
	 *                           to be updated
	 * @param htblColNameValue   key-value pairs of each column name and its new
	 *                           value
	 * @throws ClassNotFoundException when loading is not successful
	 * @throws IOException            when I/O failure occurs
	 */

	// MISSING use of index built on clustering key if exists
	public void updateBS(Comparable clusteringKeyValue, Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {

		// binary search using the clustering key column to find page index if it exists
		int lo = 0, hi = pages.size() - 1, res = -1;
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
			if (clusteringKeyValue.compareTo(max) <= 0) {
				res = mid;
				hi = mid - 1;
			} else {
				lo = mid + 1;
			}
		}

		if (res != -1) { // if page index is found then load it to memory and update it
			Page page = getPage(res);

			Tuple tuple = page.getTuple(clusteringKeyValue); // gets tuple before update
			for (GridIndex gi : indices) { // remove references of old tuple from indices
				gi.remove(tuple.getValues());
			}
			// update tuple in page and get an instance of updated tuple
			Tuple updatedTuple = page.update(clusteringKeyValue, htblColNameValue);
			// update all indices knowing the page name and knowing the position of the
			// tuple
			for (GridIndex gi : indices) {
				gi.insert(updatedTuple.getValues(), pages.get(res), page.getIndexOf(clusteringKeyValue));
			}

			save();
		} else { // if not then no tuple exists with this clustering key
			System.out.println("No such record exist");
		}
	}

	/**
	 * Deletes all tuples in table with matching criteria passed. Supports binary
	 * search if clustering key is passed, otherwise linear search is used
	 * 
	 * @param htblColNameValue key-value pairs representing the criteria of deletion
	 * @throws IOException            when I/O failure occurs
	 * @throws ClassNotFoundException when loading fails
	 */

	// MISSING remove references from the index after successful delete
	public void deleteBS(Hashtable<String, Object> htblColNameValue) throws IOException, ClassNotFoundException {
		if (htblColNameValue.containsKey(clusteringKeyColumn)) { // do binary search if clustering key value is provided
			// binary search using clustering key value
			int lo = 0, hi = pages.size() - 1, res = -1;
			Comparable value = getComparable(htblColNameValue.get(clusteringKeyColumn), clusteringKeyType);
			while (lo <= hi) {
				int mid = lo + (hi - lo) / 2;
				Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
				if (value.compareTo(max) <= 0) {
					res = mid;
					hi = mid - 1;
				} else {
					lo = mid + 1;
				}
			}

			if (res != -1) { // a page that should contain the tuple exists
				// loag page and delete the tuple in it if it exists
				Page page = getPage(res);
				page.delete(htblColNameValue.get(clusteringKeyColumn), htblColNameValue);

				// if page becomes empty after deletion then delete the page from disk
				if (page.isEmpty()) {
					deletePages(res, 1);
				}
			}
		} else { // do linear search

			// pick the best grid index to use based on columns in index
			GridIndex gridIndex = null;
			int max = 0;
			for (GridIndex gi : indices) {
				int score = 0;
				for (String colName : htblColNameValue.keySet()) {
					score += gi.getColumns().contains(colName) ? 1 : 0;
				}
				if (score > max) {
					max = score;
					gridIndex = gi;
				}
			}

			if (gridIndex == null) {
				System.out.println("NO INDEX USED IN DELETE");
			}

			if (gridIndex != null) {

				TreeMap<String, LinkedList<Integer>> pageNameRows = gridIndex.get(htblColNameValue);

				// loop over all pairs supplied
				TreeMap<String, LinkedList<Integer>> deletedPageNameRows = new TreeMap<String, LinkedList<Integer>>();
				for (Entry<String, LinkedList<Integer>> e : pageNameRows.entrySet()) {
					String pageName = e.getKey();
					// System.out.print(pageName + ": ");
					// System.out.print(e.getValue() + "\n");
					Page page = getPage(pageName);
					LinkedList<Integer> deleted = page.deleteAllIndices(e.getValue(), htblColNameValue);
					if (deleted.size() > 0) {
						deletedPageNameRows.put(pageName, deleted);
					}
					if (page.isEmpty()) {
						deletePage(pageName);
					}
				}
				for (GridIndex gi : indices) {
					gi.delete(deletedPageNameRows);
				}
				save();
			} else { // insted of loading buckets and pages, just linear search and load pages only

				// loop over available pages
				for (int i = 0; i < pages.size(); ++i) {
					// load page to memory
					Page page = getPage(i);

					// delete tuples in page with corresponding values
					page.delete(htblColNameValue);

					// if page becomes empty after deletion then delete the page from disk
					if (page.isEmpty()) {
						deletePages(i, 1);
						i--;
					}
				}
			}
		}
	}

	public Iterator select(SQLTerm[] sqlTerms, String[] ops, Hashtable<String, String> htblColNameType)
			throws IOException, ClassNotFoundException {

		// should binary search if have clustering key
		LinkedList<Tuple> ll = new LinkedList<Tuple>();

		boolean and = ops[0].toLowerCase().equals("and");

		for (int i = 0; i < pages.size(); ++i) {
			Page page = getPage(i);

			for (Tuple t : page.getTuples()) {
				if ((and && checkAND(t, sqlTerms, htblColNameType))
						|| (!and && checkOR(t, sqlTerms, htblColNameType))) {
					ll.add(t);
				}
			}
		}

		return ll.listIterator();
	}

	private boolean checkAND(Tuple tuple, SQLTerm[] sqlTerms, Hashtable<String, String> htblColNameType) {
		boolean flag = true;
		Comparable value, tValue;
		for (SQLTerm sqlTerm : sqlTerms) {
			String op = sqlTerm._strOperator;
			switch (op) {
				case "=": // special treatment with double values
					flag &= tuple.checkKeyValue(sqlTerm._strColumnName, sqlTerm._objValue);
					break;
				case "!=":
					flag &= !tuple.checkKeyValue(sqlTerm._strColumnName, sqlTerm._objValue);
					break;
				case "<":
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag &= tValue.compareTo(value) < 0;
					break;
				case ">":
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag &= tValue.compareTo(value) > 0;
					break;
				case "<=": // special treatment with doubles
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag &= tValue.compareTo(value) <= 0;
					break;
				case ">=":
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag &= tValue.compareTo(value) >= 0;
					break;
				default:
					break;
			}
		}
		return flag;
	}

	private boolean checkOR(Tuple tuple, SQLTerm[] sqlTerms, Hashtable<String, String> htblColNameType) {
		boolean flag = false;
		Comparable value, tValue;
		for (SQLTerm sqlTerm : sqlTerms) {
			String op = sqlTerm._strOperator;
			switch (op) {
				case "=": // special treatment with double values
					flag |= tuple.checkKeyValue(sqlTerm._strColumnName, sqlTerm._objValue);
					break;
				case "!=":
					flag |= !tuple.checkKeyValue(sqlTerm._strColumnName, sqlTerm._objValue);
					break;
				case "<":
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag |= tValue.compareTo(value) < 0;
					break;
				case ">":
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag |= tValue.compareTo(value) > 0;
					break;
				case "<=": // special treatment with doubles
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag |= tValue.compareTo(value) <= 0;
					break;
				case ">=":
					value = getComparable(sqlTerm._objValue, htblColNameType.get(sqlTerm._strColumnName));
					// if tuple value is null??
					tValue = getComparable(tuple.getValue(sqlTerm._strColumnName),
							htblColNameType.get(sqlTerm._strColumnName));
					flag |= tValue.compareTo(value) >= 0;
					break;
				default:
					break;
			}
		}
		return flag;
	}

	public void createIndex(String[] strarrColNames)
			throws DBAppException, ClassNotFoundException, IOException, ParseException {
		// creates a new instance of the grid index
		GridIndex gridIdx = new GridIndex(strarrColNames, htblColNameType, htblColNameMin, htblColNameMax,
				maxBucketSize, indexId++, path);

		// adds the instance to the list of indices available
		indices.add(gridIdx);

		// inserts all tuples present in table into the index to handle index creation
		// after inserting into table
		for (int i = 0; i < pages.size(); ++i) {
			Page page = getPage(i);

			int row = 0;
			for (Tuple t : page.getTuples()) {
				gridIdx.insert(t.getValues(), pages.get(i), row++);
			}
		}

		// saves the table to remember index later
		save();
	}

	/*
	 * HELPER METHODS
	 */

	private void deletePages(int stIdx, int count) throws IOException {
		for (int i = 0; i < count; ++i) {
			File f = new File(path + pages.get(stIdx));
			f.delete();
			maxKey.remove(stIdx);
			pages.remove(stIdx);
		}
		save();
	}

	private void deletePage(String pageName) throws IOException {
		File f = new File(path + pageName);
		f.delete();
		int pageIdx = pages.indexOf(pageName);
		maxKey.remove(pageIdx);
		pages.remove(pageIdx);
		save();
	}

	private void save() throws IOException {
		File f = new File(path + tableName + ".class");
		if (f.exists()) {
			f.delete();
		}
		f.createNewFile();
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(path + tableName + ".class"));
		oos.writeObject(this);
		oos.close();
	}

	private void createDirectories() {
		File file = new File(this.path);
		file.mkdirs();
	}

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

	private Page createPage() {
		return new Page(maxPageSize, path + tableName + "_" + nextPageIdx + ".class", clusteringKeyColumn,
				clusteringKeyType);
	}

	private Page getPage(int idx) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path + pages.get(idx)));
		Page page = (Page) ois.readObject();
		ois.close();
		return page;
	}

	private Page getPage(String pageName) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path + pageName));
		Page page = (Page) ois.readObject();
		ois.close();
		return page;
	}

	private boolean tupleExists(Object clusteringKeyValue) throws ClassNotFoundException, IOException {
		// binary search using max key in each page
		int lo = 0, hi = pages.size() - 1, res = -1;
		Comparable value = getComparable(clusteringKeyValue, clusteringKeyType);
		while (lo <= hi) {
			int mid = lo + (hi - lo) / 2;
			// gets max key in page as comparable
			Comparable max = getComparable(maxKey.get(mid), clusteringKeyType);
			if (value.compareTo(max) <= 0) {
				res = mid;
				hi = mid - 1;
			} else {
				lo = mid + 1;
			}
		}

		if (res != -1) { // if page exists then check page for tuple
			Page page = getPage(res);
			return page.tupleExists(value);
		} else {
			return false;
		}
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(
				"########################### TABLE " + tableName + " ###########################\n");
		for (String pageName : pages) {
			try {
				ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path + pageName));
				Page page = (Page) ois.readObject();
				sb.append(page.toString());
				ois.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

	public void printIndex(int index) {
		System.out.println(indices.get(index));
	}

	public Hashtable<String, String> getHtblColNameMin() {
		return htblColNameMin;
	}

	public Hashtable<String, String> getHtblColNameMax() {
		return htblColNameMax;
	}

	/*
	 * MAIN METHOD FOR TESTING
	 */

	public static void main(String[] args) throws IOException, ClassNotFoundException, DBAppException {
	}

}
