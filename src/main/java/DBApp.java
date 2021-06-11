import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
			if (!tableNameExists(tableName)) {
				throw new DBAppException("Couldn't complete insertion into table `" + tableName
						+ "` as no table with that name exists in the DB.");
			}

			validateColNames(columnNames, tableName);

			// loads table from memory
			Table table = loadTable(tableName);

			// creates the index for the table
			table.createIndex(columnNames);

			updateMetadataWithIndex(tableName, columnNames);
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
			// throw new DBAppException(e.getMessage());
			e.printStackTrace();
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

	public void validateColNames(String colNames[], String tableName)
			throws DBAppException, IOException, FileNotFoundException {
		HashSet<String> tableColumns = new HashSet<String>();
		BufferedReader br = new BufferedReader(new FileReader(new File(mainDir + "metadata.csv")));
		br.readLine();
		String line;
		StringTokenizer st;
		while ((line = br.readLine()) != null) {
			st = new StringTokenizer(line, ",");
			String tn = st.nextToken();
			String cn = st.nextToken();
			if (tn.equals(tableName)) {
				tableColumns.add(cn); // populates the hashtable with column names and types
			}
		}
		br.close();
		for (String col : colNames) {
			if (!tableColumns.contains(col)) {
				throw new DBAppException("Can't creat the index on table `" + tableName
						+ "` as the table does not contain the column `" + col + "`.");
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

	private void updateMetadataWithIndex(String tableName, String colNames[]) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(new File(mainDir + "metadata.csv")));
		HashSet<String> hs = new HashSet<String>();
		for (String s : colNames)
			hs.add(s);
		br.readLine();
		String line;
		StringTokenizer st;
		StringBuilder sb = new StringBuilder();
		while ((line = br.readLine()) != null) {
			st = new StringTokenizer(line, ",");
			String tn = st.nextToken();
			String cn = st.nextToken();
			String ct = st.nextToken();
			boolean isCK = Boolean.parseBoolean(st.nextToken());
			boolean isIdxed = Boolean.parseBoolean(st.nextToken()); // skips indexing for now
			String min = st.nextToken();
			String max = st.nextToken();
			if (tn.equals(tableName)) {
				sb.append(String.format("%s,%s,%s,%s,%s,%s,%s\n", tn, cn, ct, isCK, hs.contains(cn) ? "true" : "false",
						min, max));
			} else {
				sb.append(String.format("%s,%s,%s,%s,%s,%s,%s\n", tn, cn, ct, isCK, isIdxed, min, max));
			}
		}
		br.close();

		BufferedWriter bw = new BufferedWriter(new FileWriter(mainDir + "/metadata.csv", false));
		bw.append("Table Name,Column Name,Column Type,ClusteringKey,Indexed,min,max\n");
		bw.write(sb.toString());
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
}
