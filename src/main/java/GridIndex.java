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
import java.util.Map.Entry;
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
	// missing making sure that max value is included in a range
	// missing support for null values
	// missing support for overflow pages
	public GridIndex(String[] strarrColName, Hashtable<String, String> colNameType, Hashtable<String, String> minValues,
			Hashtable<String, String> maxValues, int maxBucketSize, int indexId, String path)
			throws DBAppException, ParseException {

		this.maxBucketSize = maxBucketSize;
		this.indexId = indexId;
		this.path = path;

		colNameRanges = new Hashtable<String, MinMax[]>();

		int cols = 10; // number of columns in array
		for (int i = 0; i < strarrColName.length; ++i) { // loop on each column
			// needs a little modification for Doubles
			MinMax ranges[] = null;
			if (colNameType.get(strarrColName[i]).equals("java.lang.Integer")) {
				int min = Integer.parseInt(minValues.get(strarrColName[i]));
				int max = Integer.parseInt(maxValues.get(strarrColName[i]));
				int range = max - min;
				int step = range / cols; // step value
				cols += (range % cols == 0) ? 0 : 1;
				ranges = new MinMax[cols];
				int start = min, end = min + step;
				for (int j = 0; j < cols; ++j) { // set ranges for each column
					ranges[j] = new MinMax(start, end);
					start += step;
					end += step;
				}
				if (ranges.length == cols + 1) {
					ranges[ranges.length - 1] = new MinMax(ranges[ranges.length - 2].getMin(), max);
				}
			} else if (colNameType.get(strarrColName[i]).equals("java.util.Date")) {
				SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
				Date min = sdf.parse(minValues.get(strarrColName[i]));
				Date max = sdf.parse(maxValues.get(strarrColName[i]));
				long range = (max.getTime() - min.getTime()) / (1000 * 60 * 60 * 24); // number of days difference
				int step = (int) (range / cols);
				cols += (range % cols == 0) ? 0 : 1;
				ranges = new MinMax[cols];
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

				if (ranges.length == cols + 1) {
					ranges[ranges.length - 1] = new MinMax(ranges[ranges.length - 2].getMin(), max);
				}
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
				// need to check if bucket is empty then delete it from disk
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

	private int get1DIdx(Hashtable<String, Object> htblColNameValue) {
		Hashtable<String, Integer> colNamePos = new Hashtable<String, Integer>();
		for (Entry<String, MinMax[]> e : colNameRanges.entrySet()) {
			// should be casted to appropriate data type
			Comparable value = (Comparable) htblColNameValue.get(e.getKey());

			// should be binary search
			MinMax[] range = e.getValue();
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
		for (Entry<String, Integer> e : colNamePos.entrySet()) {
			oneDIdx += (res * e.getValue());
			res *= colNameRanges.get(e.getKey()).length;
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