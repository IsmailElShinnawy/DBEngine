import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

public class GridIndex implements Serializable {

	class MinMax implements Serializable {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private Object min, max;

		MinMax(Object min, Object max) {
			this.min = min;
			this.max = max;
		}

		public Object getMin() {
			return min;
		}

		public Object getMax() {
			return max;
		}

		public String toString() {
			return min + "-" + max;
		}
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Vector<String>[] grid;
	private Hashtable<String, MinMax[]> colNameRanges;
	private int bucketIdx = 0, maxBucketSize, indexId;
	private String path;

	// missing support for String and Double columns
	// missing support for null values
	public GridIndex(String[] strarrColName, Hashtable<String, String> colNameType, Hashtable<String, String> minValues,
			Hashtable<String, String> maxValues, int maxBucketSize, int indexId, String path)
			throws DBAppException, ParseException {

		this.maxBucketSize = maxBucketSize;
		this.indexId = indexId;
		this.path = path;

		colNameRanges = new Hashtable<String, MinMax[]>();

		int cols = 10; // number of columns in array
		for (int i = 0; i < strarrColName.length; ++i) { // loop on each column
			MinMax ranges[] = null;
			if (colNameType.get(strarrColName[i]).equals("java.lang.Integer")) {
				int min = Integer.parseInt(minValues.get(strarrColName[i]));
				int max = Integer.parseInt(maxValues.get(strarrColName[i]));
				int range = max - min;
				int step = range / cols; // step value
				ranges = new MinMax[cols + (range % cols == 0 ? 0 : 1) + 1];
				int start = min, end = min + step;
				for (int j = 0; j < cols; ++j) { // set ranges for each column
					ranges[j] = new MinMax(start, end);
					start += step;
					end += step;
				}
				if (ranges.length == cols + 2) {
					ranges[ranges.length - 2] = new MinMax(ranges[ranges.length - 3].getMax(), max);
				}
				ranges[ranges.length - 1] = new MinMax(-1, -1);
			} else if (colNameType.get(strarrColName[i]).equals("java.util.Date")) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date min = sdf.parse(minValues.get(strarrColName[i]));
				Date max = sdf.parse(maxValues.get(strarrColName[i]));
				long range = (max.getTime() - min.getTime()) / (1000 * 60 * 60 * 24); // number of days difference
				int step = (int) (range / cols);
				ranges = new MinMax[cols + (range % cols == 0 ? 0 : 1) + 1];
				Date start = min;
				Calendar c = Calendar.getInstance();
				c.setTime(min);
				c.add(Calendar.DAY_OF_MONTH, step);
				Date end = c.getTime();
				for (int j = 0; j < cols; ++j) {
					ranges[j] = new MinMax(start, end);

					// start+=step
					c.setTime(start);
					c.add(Calendar.DAY_OF_MONTH, step);
					start = c.getTime();

					// end+=step
					c.setTime(end);
					c.add(Calendar.DAY_OF_MONTH, step);
					end = c.getTime();
				}

				if (ranges.length == cols + 2) {
					ranges[ranges.length - 2] = new MinMax(ranges[ranges.length - 3].getMax(), max);
				}
				ranges[ranges.length - 1] = new MinMax("", "");
			} else if (colNameType.get(strarrColName[i]).equals("java.lang.Double")) {
				double min = Double.parseDouble(minValues.get(strarrColName[i]));
				double max = Double.parseDouble(maxValues.get(strarrColName[i]));
				double range = max - min;
				double step = range / (double) cols;
				ranges = new MinMax[cols + (min + step * cols < max ? 1 : 0) + 1];
				double start = min, end = min + step;
				for (int j = 0; j < cols; ++j) {
					ranges[j] = new MinMax(start, end);
					start += step;
					end += step;
				}
				if (ranges.length == cols + 2) {
					ranges[ranges.length - 2] = new MinMax(ranges[ranges.length - 3].getMax(), max);
				}
				ranges[ranges.length - 1] = new MinMax(-1, -1);
			} else if (colNameType.get(strarrColName[i]).equals("java.lang.String")) {

			}

			colNameRanges.put(strarrColName[i], ranges);
		}

		int size = 1;
		for (Entry<String, MinMax[]> e : colNameRanges.entrySet()) {
			size *= e.getValue().length;
		}

		grid = new Vector[size];
		for (int i = 0; i < size; ++i) {
			grid[i] = new Vector<String>();
		}
	}

	public void insert(Hashtable<String, Object> htblColNameValue, String pageName, int row)
			throws ClassNotFoundException, IOException {

		int oneDIdx = get1DIdx(htblColNameValue);

		if (grid[oneDIdx].isEmpty()) {
			Bucket bucket = createBucket();
			grid[oneDIdx].add(bucket.getPath());
			bucket.insert(pageName, row);
		} else {

			boolean inserted = false;

			for (String bucketName : grid[oneDIdx]) {
				Bucket b = loadBucket(bucketName);
				if (!b.isFull()) {
					b.insert(pageName, row);
					inserted = true;
					break;
				}
			}

			if (!inserted) {
				Bucket bucket = createBucket();
				grid[oneDIdx].add(bucket.getPath());
				bucket.insert(pageName, row);
			}
		}
	}

	public void remove(Hashtable<String, Object> htblColNameValue) throws ClassNotFoundException, IOException {
		int oneDIdx = get1DIdx(htblColNameValue);
		for (String bucketName : grid[oneDIdx]) {
			Bucket b = loadBucket(bucketName);
			if (b.remove(htblColNameValue)) {
				if (b.isEmpty()) {
					deleteBucket(oneDIdx, bucketName);
				}
				return;
			}
		}
	}

	public void increment(String pageName, int idx, String ofPage, int maxPageSize)
			throws ClassNotFoundException, IOException, DBAppException {
		for (Vector<String> vector : grid) {
			for (String bucketName : vector) {
				Bucket b = loadBucket(bucketName);
				b.increment(pageName, idx, ofPage, maxPageSize);
			}
		}
	}

	public void delete(TreeMap<String, LinkedList<Integer>> deletedPageNameRows)
			throws ClassNotFoundException, IOException {
		for (int i = 0; i < grid.length; ++i) {
			Vector<String> vector = grid[i];
			for (int j = 0; j < vector.size(); ++j) {
				String bucketName = vector.get(j);
				Bucket bucket = loadBucket(bucketName);
				bucket.delete(deletedPageNameRows);
				if (bucket.isEmpty()) {
					deleteBucket(i, bucketName);
					j--;
				}
			}
		}
	}

	public TreeMap<String, LinkedList<Integer>> get(Hashtable<String, Object> htblColNameValue)
			throws ClassNotFoundException, IOException {

		Hashtable<String, Integer> htblColNameIdx = new Hashtable<String, Integer>();
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			Comparable value = (Comparable) e.getValue();
			if (colNameRanges.containsKey(e.getKey())) {
				MinMax range[] = colNameRanges.get(e.getKey());
				for (int i = 0; i < range.length; ++i) {
					if ((value.compareTo((Comparable) range[i].getMin()) >= 0
							&& value.compareTo((Comparable) range[i].getMax()) < 0)
							|| (i == range.length - 1 && value.compareTo((Comparable) range[i].getMin()) >= 0
									&& value.compareTo((Comparable) range[i].getMax()) <= 0)) {
						htblColNameIdx.put(e.getKey(), i);
						break;
					}
				}
			}
		}

		TreeMap<String, LinkedList<Integer>> pageNameRows = new TreeMap<String, LinkedList<Integer>>();

		for (int i = 0; i < grid.length; ++i) {
			Hashtable<String, Integer> map = new Hashtable<String, Integer>();
			int res = i; // represents 1D idx
			for (Entry<String, MinMax[]> e : colNameRanges.entrySet()) {
				map.put(e.getKey(), res % e.getValue().length);
				res /= e.getValue().length;
			}

			boolean flag = true;
			for (Entry<String, Integer> e : htblColNameIdx.entrySet()) {
				flag &= e.getValue().equals(map.get(e.getKey()));
			}

			if (flag) {
				for (String bucketName : grid[i]) {
					Bucket b = loadBucket(bucketName);
					for (Bucket.Pair pair : b.getRefs()) {
						if (!pageNameRows.containsKey(pair.getPageName())) {
							pageNameRows.put(pair.getPageName(), new LinkedList<Integer>());
						}
						pageNameRows.get(pair.getPageName()).add(pair.getRowNumber());
					}
				}
			}
		}

		return pageNameRows;
	}

	private int get1DIdx(Hashtable<String, Object> htblColNameValue) {
		Hashtable<String, Integer> colNamePos = new Hashtable<String, Integer>();
		for (Entry<String, MinMax[]> e : colNameRanges.entrySet()) {
			// should be casted to appropriate data
			Comparable value = (Comparable) htblColNameValue.get(e.getKey());

			// should be binary search
			MinMax[] range = e.getValue();

			if (value == null) {
				colNamePos.put(e.getKey(), range.length - 1);
				continue;
			}
			for (int i = 0; i < range.length; ++i) {
				if ((value.compareTo((Comparable) range[i].getMin()) >= 0
						&& value.compareTo((Comparable) range[i].getMax()) < 0)
						|| (i == range.length - 1 && value.compareTo((Comparable) range[i].getMin()) >= 0
								&& value.compareTo((Comparable) range[i].getMax()) <= 0)) {
					colNamePos.put(e.getKey(), i);
					break;
				}
			}
		}

		int oneDIdx = 0, res = 1;
		// for (Entry<String, Integer> e : colNamePos.entrySet()) {
		// oneDIdx += (res * e.getValue());
		// res *= colNameRanges.get(e.getKey()).length;
		// }
		for (Entry<String, MinMax[]> e : colNameRanges.entrySet()) {
			oneDIdx += (res * colNamePos.get(e.getKey()));
			res *= e.getValue().length;
		}

		return oneDIdx;
	}

	private Bucket createBucket() throws IOException {
		// bucket path: path_to_table_folder/index_[index-id]_[bucket-id].class
		return new Bucket(path, indexId, bucketIdx++, maxBucketSize);
	}

	private Bucket loadBucket(String path) throws IOException, ClassNotFoundException {
		ObjectInputStream ois = new ObjectInputStream(new FileInputStream(path));
		Bucket res = (Bucket) ois.readObject();
		ois.close();
		return res;
	}

	private void deleteBucket(int idx, String bucketName) {
		grid[idx].remove(bucketName);
		File file = new File(bucketName);
		file.delete();
	}

	public boolean isOnColumn(String colName) {
		return colNameRanges.containsKey(colName);
	}

	public Set<String> getColumns() {
		return colNameRanges.keySet();
	}

	public String toString() {
		StringBuilder sb = new StringBuilder(path + " GRID\n");
		for (Entry<String, MinMax[]> e : colNameRanges.entrySet()) {
			sb.append(e.getKey()).append(" ").append(Arrays.deepToString(e.getValue())).append("\n");
		}
		for (int i = 0; i < grid.length; ++i) {
			sb.append("Cell ").append(i).append(": ");
			if (!grid[i].isEmpty()) {
				for (String bucketName : grid[i]) {
					try {
						sb.append(loadBucket(bucketName).toString()).append("\n");
					} catch (ClassNotFoundException | IOException e) {
						e.printStackTrace();
					}
				}
			} else {
				sb.append("empty cell\n");
			}
		}

		return sb.toString();
	}

	public static void main(String[] args) {
	}
}