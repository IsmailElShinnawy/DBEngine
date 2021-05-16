import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.StringTokenizer;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class DBApp implements DBAppInterface {

	private int maximumRowsCountinPage, maximumRowsCountinBucket;
	private HashSet<String> allowedTypes;
	private boolean firstTable = true;

	private final String mainDir = "src/main/resources/";

	/**
	 * Initiates the DB application by loading relevant info from the config file
	 * and initiating the supported data types
	 */
	@Override
	public void init() {

		// reads config file and sets the maximumRowsCountInPage
		Config config = new Config(mainDir + "DBApp.config");
		maximumRowsCountinPage = Integer.parseInt(config.getProperty("MaximumRowsCountinPage"));
		maximumRowsCountinBucket = Integer.parseInt(config.getProperty("MaximumKeysCountinIndexBucket"));

		// add allowed types supported by the DB engine to hashset for validation of
		// input
		allowedTypes = new HashSet<String>();
		allowedTypes.add("java.lang.Integer");
		allowedTypes.add("java.lang.String");
		allowedTypes.add("java.lang.Double");
		allowedTypes.add("java.util.Date");
	}

	/**
	 * creates a table in the database
	 * 
	 * @param tableName     the name of the table to be created
	 * @param clusteringKey the name of the column to be used as a primary and
	 *                      clustering key in the table
	 * @param colNameType   a hashtable holding every column name and its
	 *                      corresponding data type
	 * @param colNameMin    a hashtable holding every column name and its
	 *                      corresponding minimum value
	 * @param colNameMax    a hashtable holding every column name and its
	 *                      corresponding maximum value
	 * @throws DBAppException when a table with the same name already exists,
	 *                        clustering key is not valid or data types provided is
	 *                        not supported
	 */
	@Override
	public void createTable(String tableName, String clusteringKey, Hashtable<String, String> colNameType,
			Hashtable<String, String> colNameMin, Hashtable<String, String> colNameMax) throws DBAppException {
		try {

			// if a table with the name already exists in the DB don't allow it
			if (tableNameExists(tableName)) {
				throw new DBAppException("Couldn't create table `" + tableName
						+ "` as a table with the same name already exists in the DB");
			}

			// checks if clusteringKey is a valid column name in the table
			if (!colNameType.containsKey(clusteringKey)) {
				throw new DBAppException("Couldn't create table `" + tableName + "` as the column `" + clusteringKey
						+ "` does not exist in table and is set to clustering key");
			}

			// checks that column types are supported by the DB engine
			validateTypes(colNameType, tableName);

			// checks that column names in min and max hashtables belong to the table and
			// min/max values match data types
			for (String col : colNameType.keySet()) {
				if (!colNameMin.containsKey(col)) {
					throw new DBAppException("Couldn't create table `" + tableName + "` as the column `" + col
							+ "` is not assigned a min value");
				}
				if (!colNameMax.containsKey(col)) {
					throw new DBAppException("Couldn't create table `" + tableName + "` as the column `" + col
							+ "` is not assigned a max value");
				}
			}

			for (Entry<String, String> e : colNameMin.entrySet()) {
				if (!colNameType.containsKey(e.getKey())) {
					throw new DBAppException("Couldn't create table `" + tableName + "` as the column `" + e.getKey()
							+ "` in the colNameMin hashtable doesn't exist in the table");
				}

				try {
					getAsComparable(e.getValue(), colNameType.get(e.getKey()));
				} catch (Exception exc) {
					throw new DBAppException("Couldn't create table `" + tableName + "` because min value for column `"
							+ e.getKey() + "` is set to `" + e.getValue() + "` which is not compatible with `"
							+ colNameType.get(e.getKey()) + "`");
				}
			}

			for (Entry<String, String> e : colNameMax.entrySet()) {
				if (!colNameType.containsKey(e.getKey())) {
					throw new DBAppException("Couldn't create table `" + tableName + "` as the column `" + e.getKey()
							+ "` in the colNameMax hashtable doesn't exist in the table");
				}

				try {
					getAsComparable(e.getValue(), colNameType.get(e.getKey()));
				} catch (Exception exc) {
					throw new DBAppException("Couldn't create table `" + tableName + "` because max value for column `"
							+ e.getKey() + "` is set to `" + e.getValue() + "` which is not compatible with `"
							+ colNameType.get(e.getKey()) + "`");
				}
			}

			// creates table using its constructor
			new Table(tableName, clusteringKey, colNameType, colNameMin, colNameMax, mainDir, maximumRowsCountinPage,
					maximumRowsCountinBucket);

			// adds rows to the metadata file corresponding to the created table
			updateMetaDataFile(tableName, colNameType, colNameMin, colNameMax, clusteringKey);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

	}

	@Override
	public void createIndex(String tableName, String[] columnNames) throws DBAppException {
		try {
			// MISSING:
			// validation for tableName
			// validation for column names
			// validation for duplicate indices (indices on the same columns)

			// loads table from memory
			Table table = loadTable(tableName);

			// creates the index for the table
			table.createIndex(columnNames);
		} catch (ClassNotFoundException | IOException | ParseException e) {
			e.printStackTrace();
		}
	}

	/**
	 * inserts a new tuple
	 * 
	 * @param tableName    the name of the table to be used for insertion
	 * @param colNameValue a hashtable holding the tuple to be inserted as key-value
	 *                     pairs
	 * @throws DBAppException when table does not exist in DB or input tuple is not
	 *                        valid
	 */
	@Override
	public void insertIntoTable(String tableName, Hashtable<String, Object> colNameValue) throws DBAppException {
		try {
			// checks if this table exists and if not doesn't allow insertion
			if (!tableNameExists(tableName)) {
				throw new DBAppException("Couldn't complete insertion into table `" + tableName
						+ "` as no table with that name exists in the DB.");
			}

			// checks that input contains clustering key and that column values are of the
			// correct type and within [Min, Max] range
			validateInput(colNameValue, tableName, true, false);

			// load table into memory and insert tuple
			Table table = loadTable(tableName);

			table.insertWithOF(colNameValue);
			table = null;
		} catch (IOException | ClassNotFoundException | NumberFormatException | ParseException e) {
			throw new DBAppException(e.getMessage());
		}
	}

	/**
	 * updates tuple in the table with the specified key to the new values passed
	 * 
	 * @param tableName          the name of the table to be updated
	 * @param clusteringKeyValue the key of the tuple to be updated
	 * @param colNameValue       key-value pairs representing each column name and
	 *                           its updated value
	 * @throws DBAppException when table does not exist in DB, clustering key is not
	 *                        valid or update values are not valid
	 */
	@Override
	public void updateTable(String tableName, String clusteringKeyValue, Hashtable<String, Object> colNameValue)
			throws DBAppException {
		try {
			// checks if this table exists and if not doesn't allow insertion
			if (!tableNameExists(tableName)) {
				throw new DBAppException("Couldn't complete updating table `" + tableName
						+ "` as no table with that name exists in the DB.");
			}

			// validate the clustering key type and if valid it is returned as an comparable
			// instead of a string and checks that clustering key is not updated
			Comparable objClusteringKeyValue = validateCK(clusteringKeyValue, colNameValue, tableName);

			// checks that that column values are of the correct type and within [Min, Max]
			// range
			validateInput(colNameValue, tableName, false, false);

			// load table to memory and update the tuple if exists
			Table table = loadTable(tableName);

			table.updateBS(objClusteringKeyValue, colNameValue);
			table = null;
		} catch (IOException | ClassNotFoundException | ParseException e) {
			throw new DBAppException(e.getMessage());
		}
	}

	/**
	 * deletes tuple in the table with the corresponding criteria
	 * 
	 * @param tableName    the name of the table to be used in deletion
	 * @param colNameValue key-value pairs representing each column name and its
	 *                     criteria value
	 * @throws DBAppException when table does not exist in DB, criteria values are
	 *                        not valid
	 */
	@Override
	public void deleteFromTable(String tableName, Hashtable<String, Object> columnNameValue) throws DBAppException {
		try {
			// checks if this table exists and if not doesn't allow insertion
			if (!tableNameExists(tableName)) {
				throw new DBAppException("Couldn't complete deletion from table `" + tableName
						+ "` as no table with that name exists in the DB.");
			}

			// checks that that column values are of the correct type and within [Min, Max]
			// range
			validateInput(columnNameValue, tableName, false, true);

			// load table to memory and delete the tuples if any exists
			Table table = loadTable(tableName);

			table.deleteBS(columnNameValue);
			table = null;
		} catch (IOException | ClassNotFoundException | NumberFormatException | ParseException e) {
			throw new DBAppException(e.getMessage());
		}
	}

	@Override
	public Iterator selectFromTable(SQLTerm[] sqlTerms, String[] arrayOperators) throws DBAppException {
		try {
			String tableName = sqlTerms[0]._strTableName; // assuming that only one table could be used at an instance

			Table table = loadTable(tableName);

			Iterator itr = table.select(sqlTerms, arrayOperators, getColNameType(tableName));

			return itr;

		} catch (ClassNotFoundException | IOException e) {
			// throw new DBAppException(e.getMessage());
			e.printStackTrace();
		}
		return null;
	}

	private Hashtable<String, String> getColNameType(String tableName) throws IOException {
		String line = "";
		BufferedReader br = new BufferedReader(new FileReader(mainDir + "metadata.csv"));
		Hashtable<String, String> res = new Hashtable<String, String>();
		while ((line = br.readLine()) != null) {
			StringTokenizer st = new StringTokenizer(line, ",");
			String tn = st.nextToken();
			String cn = st.nextToken();
			String ct = st.nextToken();
			if (tableName.equals(tn)) {
				res.put(cn, ct);
			}
		}
		br.close();
		return res;
	}

	/**
	 * checks if a table with such name exists on disk or not
	 * 
	 * @param strTableName table name to be checked
	 * @return true if table exists, otherwise returns false
	 */
	private boolean tableNameExists(String strTableName) {
		File file = new File(mainDir + "data/" + strTableName);
		return file.exists();
	}

	/**
	 * checks if required column data types are supported by the DB engine
	 * 
	 * @param htblColNameType key-value pairs representing the column name and its
	 *                        corresponding data type
	 * @param tableName       the name of the table currently being checked
	 * @throws DBAppException when some column data type is not supported by the DB
	 *                        engine
	 */
	private void validateTypes(Hashtable<String, String> htblColNameType, String tableName) throws DBAppException {
		for (Entry<String, String> e : htblColNameType.entrySet()) {
			if (!allowedTypes.contains(e.getValue())) {
				throw new DBAppException("Couldn't create table `" + tableName + "` as column `" + e.getKey()
						+ "` is set to `" + e.getValue() + "` which is not supported by the DB");
			}
		}
	}

	/**
	 * checks if tuple matches info from metadata, checks if values passed are
	 * matching with data types, if column names exist in table, if values are
	 * within specified range and if inserting validates that clustering key is not
	 * set to null
	 * 
	 * @param htblColNameValue key-value pairs representing the tuple
	 * @param strTableName     the name of the table currently being checked
	 * @param boolIns          is it an insertion?
	 * @throws DBAppException         when cannot complete database operation due to
	 *                                wrong/missing input
	 * @throws IOException            when I/O failures happen
	 * @throws ClassNotFoundException when reflection error occurs
	 * @throws NumberFormatException  when parse exception occurs
	 * @throws ParseException         when parse exception occurs
	 */
	private void validateInput(Hashtable<String, Object> htblColNameValue, String strTableName, boolean boolIns,
			boolean boolDel)
			throws DBAppException, IOException, ClassNotFoundException, NumberFormatException, ParseException {
		// load table column names, types, max and min from metadata into a hashtable
		Hashtable<String, String> colNameType = new Hashtable<String, String>();
		Hashtable<String, String> colNameMin = new Hashtable<String, String>();
		Hashtable<String, String> colNameMax = new Hashtable<String, String>();

		BufferedReader br = new BufferedReader(new FileReader(new File(mainDir + "metadata.csv")));
		br.readLine();
		String line, clusteringKey = "";
		StringTokenizer st;
		while ((line = br.readLine()) != null) {
			st = new StringTokenizer(line, ",");
			String tn = st.nextToken();
			String cn = st.nextToken();
			String ct = st.nextToken();
			boolean isCK = Boolean.parseBoolean(st.nextToken());
			st.nextToken(); // skips indexing for now
			String min = st.nextToken();
			String max = st.nextToken();
			if (tn.equals(strTableName)) {
				colNameType.put(cn, ct); // populates the hashtable with column names and types
				colNameMin.put(cn, min); // populates the hashtable with column names and min
				colNameMax.put(cn, max); // populates the hashtable with column names and max
				if (isCK) {
					clusteringKey = cn; // saves the name of the clustering key
				}
			}
		}
		br.close();

		// if insert is validated checks if clustering key is provided as NULL values
		// for CK is not supported
		if (boolIns && !htblColNameValue.containsKey(clusteringKey)) {
			throw new DBAppException("Can't complete insertion into table `" + strTableName + "` as column `"
					+ clusteringKey + "` is not set and it's the clustering key.");
		}

		// checks that all column names provided are valid column names in the table and
		// that column values provided are compatible with the column types and are
		// within the accepted range
		for (Entry<String, Object> e : htblColNameValue.entrySet()) {
			if (!colNameType.containsKey(e.getKey())) {
				throw new DBAppException("Can't complete operation on table `" + strTableName + "` as column `"
						+ e.getKey() + "` doesn't exist in table.");
			}

			Class c = Class.forName(colNameType.get(e.getKey()));
			if (!c.isInstance(e.getValue())) {
				throw new DBAppException("Can't complete operation on table `" + strTableName + "` as column `"
						+ e.getKey() + "` is set to `" + e.getValue() + "` which is not compatible with `"
						+ colNameType.get(e.getKey()) + "`");
			}

			Comparable minC = getAsComparable(colNameMin.get(e.getKey()), colNameType.get(e.getKey()));
			Comparable maxC = getAsComparable(colNameMax.get(e.getKey()), colNameType.get(e.getKey()));
			Comparable insValue = getAsComparable(e.getValue(), colNameType.get(e.getKey()));

			if (!boolDel && (insValue.compareTo(minC) < 0 || insValue.compareTo(maxC) > 0)) {
				throw new DBAppException("Can't complete operation on table `" + strTableName + "` as column `"
						+ e.getKey() + "` is set to `" + e.getValue() + "` which is out of accepted range of ["
						+ colNameMin.get(e.getKey()) + "," + colNameMax.get(e.getKey()) + "]");
			}
		}
	}

	/**
	 * checks if clustering key value is a valid one
	 * 
	 * @param ckValue   a string representation for the clustering key
	 * @param tableName the name of the table currently being checked
	 * @return the clustering key value as a comparable
	 * @throws DBAppException
	 * @throws IOException
	 * @throws ParseException
	 * @throws ClassNotFoundException
	 */
	public Comparable validateCK(String ckValue, Hashtable<String, Object> htblColNameValue, String tableName)
			throws DBAppException, IOException, ParseException, ClassNotFoundException {
		// reads the metadata file to extract the clustering key type
		BufferedReader br = new BufferedReader(new FileReader(new File(mainDir + "metadata.csv")));
		br.readLine();
		String line, cn = "", ckType = "";
		StringTokenizer st;
		while ((line = br.readLine()) != null) {
			st = new StringTokenizer(line, ",");
			String tn = st.nextToken();
			cn = st.nextToken();
			String ct = st.nextToken();
			boolean isCK = Boolean.parseBoolean(st.nextToken());
			if (tn.equals(tableName) && isCK) {
				ckType = ct;
				break;
			}
		}
		br.close();

		// checks that the clustering key is not updated
		if (htblColNameValue.containsKey(cn)) {
			throw new DBAppException("Can't complete operation on table `" + tableName
					+ "` as it is not allowed to update the clustering key.");
		}

		// try to parse clustering key string to clustering key type if parse fails then
		// don't allow operation
		Comparable ckObject = null;
		try {
			ckObject = getAsComparable(ckValue, ckType);
		} catch (Exception e) {
			throw new DBAppException("Can't complete operation on table `" + tableName
					+ "` as clustering key is set to `" + ckValue + "` which is not compatible with `" + ckType + "`");
		}
		return ckObject;
	}

	private Comparable getAsComparable(String strValue, String strType) throws ParseException, NumberFormatException {
		Comparable res = null;
		switch (strType) {
			case "java.lang.Integer":
				res = Integer.parseInt(strValue);
				break;
			case "java.lang.Double":
				res = Double.parseDouble(strValue);
				break;
			case "java.lang.String":
				res = strValue;
				break;
			case "java.util.Date":
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
				res = df.parse(strValue);
				break;
			default:
				break;
		}
		return res;
	}

	private Comparable getAsComparable(Object objValue, String strType) {
		Comparable res = null;
		switch (strType) {
			case "java.lang.Integer":
				res = (Integer) objValue;
				break;
			case "java.lang.Double":
				res = (Double) objValue;
				break;
			case "java.lang.String":
				res = (String) objValue;
				break;
			case "java.util.Date":
				res = (Date) objValue;
				break;
			default:
				break;
		}
		return res;
	}

	private void updateMetaDataFile(String strTableName, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax,
			String strClusteringKeyColumn) throws IOException {
		BufferedWriter bw = new BufferedWriter(new FileWriter(mainDir + "/metadata.csv", true));
		if (firstTable) {
			bw.append("Table Name,Column Name,Column Type,ClusteringKey,Indexed,min,max\n");
			firstTable = false;
		}
		for (Entry<String, String> entry : htblColNameType.entrySet()) {
			String colName = entry.getKey();
			String colType = entry.getValue();
			boolean isClustering = colName.equals(strClusteringKeyColumn);
			boolean indexed = false;
			String min = htblColNameMin.get(colName);
			String max = htblColNameMax.get(colName);
			String line = strTableName + "," + colName + "," + colType + "," + isClustering + "," + indexed + "," + min
					+ "," + max + "\n";
			bw.append(line);
		}
		bw.close();
	}

	public void printTable(String tableName) throws IOException, ClassNotFoundException {
		System.out.println(loadTable(tableName));
	}

	public void printIndexFromTable(int idxId, String tableName) throws ClassNotFoundException, IOException {
		loadTable(tableName).printIndex(idxId);
	}

	private Table loadTable(String strTableName) throws ClassNotFoundException, IOException {
		ObjectInputStream ois = new ObjectInputStream(
				new FileInputStream(mainDir + "data/" + strTableName + "/" + strTableName + ".class"));
		Table res = (Table) ois.readObject();
		ois.close();
		return res;
	}

	public static void main(String[] args) throws DBAppException, ClassNotFoundException, IOException, ParseException {
		// initiate new instance of the DBApp
		DBApp dbApp = new DBApp();
		dbApp.init();

		// creates new students table

		// colNameType breaks engine
		// 1. if type is not supported (done)

		// Hashtable<String, String> colNameType = new Hashtable<String, String>();
		// colNameType.put("id", "java.lang.Integer");
		// colNameType.put("name", "java.lang.String");
		// colNameType.put("gpa", "java.lang.Double");
		// colNameType.put("dob", "java.util.Date");
		// colNameType.put("graduated", "java.lang.Boolean"); // should throw exception
		// until it is commented out

		// colNameMin and colNameMax breaks engine if
		// 1. missing/additional colNames (done)
		// 2. non matching min/max values with data types (done)

		// Hashtable<String, String> colNameMin = new Hashtable<String, String>();
		// colNameMin.put("id", "0");
		// colNameMin.put("name", "A");
		// colNameMin.put("gpa", ".7");
		// colNameMin.put("dob", "1999-01-01");
		// colNameMin.put("id", "throwException"); // should throw exception until it is
		// commented out
		// colNameMin.put("gpa", "throwException"); // should throw exception until it
		// is commented out
		// colNameMin.put("dob", "throwException"); // should throw exception until it
		// is commented out
		// colNameMin.put("nonExistentCol", "test"); // should throw exception until it
		// is commented out

		// Hashtable<String, String> colNameMax = new Hashtable<String, String>();
		// colNameMax.put("id", "1000");
		// colNameMax.put("name", "zzzzzzzzzzzzz");
		// colNameMax.put("gpa", "5");
		// colNameMax.put("dob", "2023-12-31");
		// colNameMax.put("id", "throwException"); // should throw exception until it is
		// commented out
		// colNameMax.put("gpa", "throwException"); // should throw exception until it
		// is commented out
		// colNameMax.put("dob", "throwException"); // should throw exception until it
		// is commented out
		// colNameMax.put("nonExistentCol", "test"); // should throw exception until it
		// is commented out

		// creating breaks engine
		// 1. if clustering key is not a valid one (done)
		// 2. if table with the same name already exists (done)

		// dbApp.createTable("students", "nonExistentCol", colNameType, colNameMin,
		// colNameMax); // should be commented out
		// dbApp.createTable("students", "id", colNameType, colNameMin, colNameMax);
		// dbApp.createTable("students", "gpa", colNameType, colNameMin, colNameMax);
		// dbApp.createTable("students", "name", colNameType, colNameMin, colNameMax);
		// dbApp.createTable("students", "dob", colNameType, colNameMin, colNameMax);

		// inserts into students table

		// 1. inserts single tuple

		// colNameValue breaks insert if
		// 1. clustering key is not supplied (done)
		// 2. additional colNames are used (done)
		// 3. min/max is violated (done)
		// 4. type is violated (done)
		// 5. clustering key uniqueness is violated (done)

		// Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("id", new Integer(1)); // should throw exception until line
		// is uncommented
		// colNameValue.put("name", "student1");
		// colNameValue.put("gpa", new Double(.98));
		// colNameValue.put("dob", new Date());

		// colNameValue.put("nonExistentCol", "throwException"); // should throw
		// exception until line is commented out

		// min/max violation // should throw exception until all lines are commented out

		// colNameValue.put("id", new Integer(-1));
		// colNameValue.put("id", new Integer(1001));
		// colNameValue.put("name", "");
		// colNameValue.put("name", "zzzzzzzzzzzzzz");
		// colNameValue.put("gpa", new Double(.1));
		// colNameValue.put("gpa", new Double(7));
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		// Date date = sdf.parse("1998-01-01");
		// colNameValue.put("dob", date);
		// Date date = sdf.parse("2024-01-01");
		// colNameValue.put("dob", date);

		// type violation // should throw exception until all lines are commented out

		// colNameValue.put("id", "throwException");
		// colNameValue.put("name", new Integer(1));
		// colNameValue.put("gpa", new Date());
		// colNameValue.put("dob", "throwException");

		// breaks engine
		// 1. if no table with name supplied (done)

		// dbApp.insertIntoTable("nonExistentTableName", colNameValue); // should throw
		// exception until it is commented out
		// dbApp.insertIntoTable("students", colNameValue);

		// 2. insert multiple tuples to check sorting and overflowing
		// sorting:
		// 1. name (String) (done)
		// 2. gpa (Double) (done)
		// 3. id (Integer) (done)
		// 4. dob (Date) (done)
		// note: adjust page size in .config to 50 before creating table to check
		// overflow pages

		// for(int i = 150; i > 1; --i) {
		// colNameValue = new Hashtable<String, Object>();
		// int id = i;
		//// int id = (int) (Math.random() * 150); // inserts random ids
		// colNameValue.put("id", new Integer(id));
		// colNameValue.put("name", "student_" + id);
		// colNameValue.put("gpa", Math.random() + .7);
		// colNameValue.put("dob", new Date());
		// try {
		// dbApp.insertIntoTable("students", colNameValue);
		// } catch(DBAppException dbe) {
		// System.out.println(dbe.getMessage());
		// }
		// }

		// dbApp.printTable("students");

		// test for date field is set to be the clustering key
		// for(int i = 31; i > 0; --i) {
		// colNameValue = new Hashtable<String, Object>();
		// int id = i;
		//// int id = (int) (Math.random() * 150); // inserts random ids
		// colNameValue.put("id", new Integer(id));
		// colNameValue.put("name", "student_" + id);
		// colNameValue.put("gpa", Math.random() + .7);
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		// Date date = sdf.parse("2000-05-" + id);
		// colNameValue.put("dob", date);
		// try {
		// dbApp.insertIntoTable("students", colNameValue);
		// } catch(DBAppException dbe) {
		// System.out.println(dbe.getMessage());
		// }
		// }

		// should throw exception for duplicate dob if dob is clustering key
		// colNameValue.put("id", new Integer(31));
		// colNameValue.put("name", "student_" + 31);
		// colNameValue.put("gpa", Math.random() + .7);
		// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		// Date date = sdf.parse("2000-05-31");
		// colNameValue.put("dob", date);
		// dbApp.insertIntoTable("students", colNameValue);

		// dbApp.printTable("students");

		// updates students table

		// colNameValue breaks insert if
		// 1. clustering key is updated (done)
		// 2. additional colNames are used (done)
		// 3. min/max is violated (done)
		// 4. type is violated (done)

		// colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("name", "updatedValue");
		// colNameValue.put("gpa", new Double(5.0));

		// colNameValue.put("id", new Integer(-1)); // should throw exception until line
		// commented out

		// colNameValue.put("nonExistingCol", new Double(5.0)); // should throw
		// exception until line commented out

		// colNameValue.put("name", ""); // should throw exception until line commented
		// out
		// colNameValue.put("name", "zzzzzzzzzzzzzz"); // should throw exception until
		// line commented out

		// colNameValue.put("name", new Integer(1)); // should throw exception until
		// line commented out

		// breaks engine if
		// 1. invalid clustering key value (type wise) (done)
		// 2. invalid clustering key value (min/max or not found wise?)
		// 3. invalid table name

		// dbApp.updateTable("students", "throwException", colNameValue); // should
		// throw exception until line commented out
		// dbApp.updateTable("students", "50", colNameValue);
		// dbApp.printTable("students");

		// delete from table

		// for(int i = 0; i < 25; ++i) {
		// colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("id", i);
		// colNameValue.put("name", "Student_" + i);
		// colNameValue.put("gpa", .7);
		// colNameValue.put("dob", new Date());
		// dbApp.insertIntoTable("students", colNameValue);
		// }
		//
		// for(int i = 0; i < 25; ++i) {
		// int id = i + 25;
		// colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("id", id);
		// colNameValue.put("name", "delete");
		// colNameValue.put("gpa", .8);
		// colNameValue.put("dob", new Date());
		// dbApp.insertIntoTable("students", colNameValue);
		// }
		//
		// for(int i = 0; i < 25; ++i) {
		// int id = i + 50;
		// colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("id", id);
		// colNameValue.put("name", "delete");
		// colNameValue.put("gpa", .9);
		// colNameValue.put("dob", new Date());
		// dbApp.insertIntoTable("students", colNameValue);
		// }
		//
		// dbApp.printTable("students");

		// colNameValue breaks insert if
		// 1. additional colNames are used (done)
		// 2. type is violated (done)

		// colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("id", 1);
		// colNameValue.put("name", "delete"); // should not delete
		// colNameValue.put("gpa", .8);

		// colNameValue.put("NonExistentCol", .8); // should throw exception until line
		// commented out
		// colNameValue.put("name", new Integer(1)); // should throw exception until
		// line commented out

		// breaks engine if
		// 1. table name does not exist
		// dbApp.deleteFromTable("students", colNameValue);

		// dbApp.printTable("students");

		// testing select from table without index

		// for(int i = 0; i < 150; ++i) {
		// colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("id", i);
		// colNameValue.put("name", "student" + i);
		// int month = (int) (Math.random() * 12) + 1;
		// int day = (int) (Math.random() * 31) + 1;
		// colNameValue.put("dob", new Date(2000 - 1900, month - 1, day));
		// colNameValue.put("gpa", Math.random() + .7);
		// dbApp.insertIntoTable("students", colNameValue);
		// }

		// dbApp.printTable("students");
		//
		// SQLTerm sqlTerms[] = new SQLTerm[2];
		//
		// sqlTerms[0] = new SQLTerm();
		// sqlTerms[0]._strTableName = "students";
		// sqlTerms[0]._strColumnName = "id";
		// sqlTerms[0]._strOperator = ">";
		// sqlTerms[0]._objValue = -1;
		//
		// sqlTerms[1] = new SQLTerm();
		// sqlTerms[1]._strTableName = "students";
		// sqlTerms[1]._strColumnName = "dob";
		// sqlTerms[1]._strOperator = ">";
		// sqlTerms[1]._objValue = new Date(2000 - 1900, 4 - 1, 31 - 1);
		//
		// Iterator itr = dbApp.selectFromTable(sqlTerms, new String[] {"AND"});
		//
		// System.out.println("----------------------------------------------");
		//
		// while(itr.hasNext()) {
		// System.out.println(itr.next());
		// }

		// testing insert using index

		Hashtable<String, String> colNameType = new Hashtable<String, String>();

		colNameType.put("col-A", "java.lang.Integer");
		colNameType.put("col-B", "java.lang.Integer");
		colNameType.put("col-C", "java.lang.Integer");
		colNameType.put("col-D", "java.util.Date");

		Hashtable<String, String> colNameMin = new Hashtable<String, String>();
		colNameMin.put("col-A", "0");
		colNameMin.put("col-B", "0");
		colNameMin.put("col-C", "0");
		colNameMin.put("col-D", "2000-1-1");

		Hashtable<String, String> colNameMax = new Hashtable<String, String>();
		colNameMax.put("col-A", "150");
		colNameMax.put("col-B", "155");
		colNameMax.put("col-C", "150");
		colNameMax.put("col-D", "2000-12-31");

		dbApp.createTable("test_table", "col-A", colNameType, colNameMin, colNameMax);
		dbApp.createIndex("test_table", new String[] { "col-B", "col-D" });
		// dbApp.createIndex("test_table", new String[] { "col-C" });
		// dbApp.createIndex("test_table", new String[] { "col-B" });

		// for (int i = 0; i < 15; ++i) {
		// int colA = i;
		// int colB = i;
		// int colC = (int) (Math.random() * 150);
		// Date colD = null;
		// if (colB == 10) {
		// colD = new Date(2000 - 1900, 4 - 1, 10);
		// } else {
		// colD = new Date(2000 - 1900, (int) (Math.random() * 12), (int) (Math.random()
		// * 28) + 1);
		// }
		// Hashtable<String, Object> colNameValue = new Hashtable<String, Object>();
		// colNameValue.put("col-A", colA);
		// colNameValue.put("col-B", colB);
		// colNameValue.put("col-C", colC);
		// colNameValue.put("col-D", colD);
		// try {
		// dbApp.insertIntoTable("test_table", colNameValue);
		// } catch (Exception e) {
		// System.out.println(e.getMessage());
		// }
		// }

		// dbApp.printTable("test_table");

		// dbApp.createIndex("test_table", new String[] { "col-B", "col-D" });

		// dbApp.printIndexFromTable(0, "test_table"); // prints index created before
		// insertion
		// System.out.println("-----------------------------------------------------------");
		// dbApp.printIndexFromTable(1, "test_table"); // prints index created after
		// insertion

		// tests update with index

		// Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		// htblColNameValue.put("col-A", 1);
		// htblColNameValue.put("col-B", 1);
		// htblColNameValue.put("col-C", 1);
		// htblColNameValue.put("col-D", new Date(2000 - 1900, 5 - 1, 4));

		// dbApp.insertIntoTable("test_table", htblColNameValue);

		// dbApp.printTable("test_table");
		// dbApp.printIndexFromTable(0, "test_table");

		// htblColNameValue.clear();
		// htblColNameValue.put("col-B", 16);
		// htblColNameValue.put("col-D", new Date(2000 - 1900, 5 - 1, 4));

		// dbApp.updateTable("test_table", "1", htblColNameValue);

		// dbApp.printTable("test_table");

		// dbApp.printIndexFromTable(0, "test_table");

		// test delete with index

		// Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
		// htblColNameValue.put("col-B", 10);
		// htblColNameValue.put("col-C", 1);
		// htblColNameValue.put("col-D", new Date(2000 - 1900, 4 - 1, 10));

		// dbApp.deleteFromTable("test_table", htblColNameValue);

		for (int i = 0; i < 4; ++i) {
			int colA = i;
			int colB = 1;
			int colC = i;
			Date colD = new Date(2000 - 1900, 1 - 1, 10);
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			htblColNameValue.put("col-A", colA);
			htblColNameValue.put("col-B", colB);
			htblColNameValue.put("col-C", colC);
			htblColNameValue.put("col-D", colD);

			dbApp.insertIntoTable("test_table", htblColNameValue);
		}

		for (int i = 0; i < 6; ++i) {
			int colA = i + 4;
			int colB = 15;
			int colC = colA;
			Date colD = new Date(2000 - 1900, 1 - 1, 10);
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			htblColNameValue.put("col-A", colA);
			htblColNameValue.put("col-B", colB);
			htblColNameValue.put("col-C", colC);
			htblColNameValue.put("col-D", colD);

			dbApp.insertIntoTable("test_table", htblColNameValue);
		}

		for (int i = 0; i < 5; ++i) {
			int colA = i + 10;
			int colB = 19;
			int colC = colA;
			Date colD = new Date(2000 - 1900, 6 - 1, 30);
			Hashtable<String, Object> htblColNameValue = new Hashtable<String, Object>();
			htblColNameValue.put("col-A", colA);
			htblColNameValue.put("col-B", colB);
			htblColNameValue.put("col-C", colC);
			htblColNameValue.put("col-D", colD);

			dbApp.insertIntoTable("test_table", htblColNameValue);
		}

		dbApp.printTable("test_table");
		dbApp.printIndexFromTable(0, "test_table");
		// dbApp.printIndexFromTable(1, "test_table");
		// dbApp.printIndexFromTable(2, "test_table");

		Hashtable<String, Object> delete = new Hashtable<String, Object>();
		delete.put("col-C", 3);
		delete.put("col-D", new Date(2000 - 1900, 1 - 1, 10));
		// delete.put("col-C", 1);

		dbApp.deleteFromTable("test_table", delete);

		dbApp.printTable("test_table");

	}

}
