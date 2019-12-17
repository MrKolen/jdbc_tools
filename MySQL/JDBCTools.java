package MySQL;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class JDBCTools {
    private static final String JDBC_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static String DB_URL = null;
    private static String USER = null;
    private static String PASS = null;
    private static Connection conn;
    private static Statement statement;
    private static ResultSet resultSet;
    private static PreparedStatement preparedStatement;

    public JDBCTools(String ipAddress, String port, String database, String userName, String password) {
        DB_URL = "jdbc:mysql://" + ipAddress + ":" + port + "/" + database + "?useSSL=false&serverTimezone=GMT%2B8";  // TimeZone 会影响Java读取时间偏差
        USER = userName;
        PASS = password;
    }

    /**
     * 与数据库建立连接
     */
    private Connection createConnection() {

        try {
            Class.forName(JDBC_DRIVER);
            System.out.println("Successfully loading database driver.");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        try {
            conn = DriverManager.getConnection(DB_URL, USER, PASS);
            System.out.println("Successfully connected to database.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 断开与数据库的连接
     * @param connection Connection接口
     * @param preparedStatement PreparedStatement接口
     * @param resultSet ResultSet接口
     * @see SQLException
     */
    private static void releaseConnection(Connection connection, Statement statement, PreparedStatement preparedStatement, ResultSet resultSet) {
        try {
            if (resultSet != null) {
                resultSet.close();
            }
            if (statement!= null) {
                statement.close();
            }
            if (preparedStatement!= null) {
                preparedStatement.close();
            }
            if (connection != null) {
                connection.close();
            }
            System.out.println("Resource closed.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 通过SQL语言查询数据：SELECT col1, col2, ... FROM table WHERE ... .
     * @param sql SQL查询语句.
     * @param params 占位符参数列表, param1, param2, param3, ... .
     * @return listMapResult, 一个map的list集合, map中Key为列名,Value为对应值.
     * @see SQLException
     */
    public List<Map<String, Object>> getDataBySQL(String sql, List<Object> params) {
        List<Map<String, Object>> listMapResult = new ArrayList<>();
        System.out.println(sql);
        int index = 1;  // 占位符索引
        conn = createConnection();
        try {
            preparedStatement = conn.prepareStatement(sql);
            for (Object placeholder : params) {
                preparedStatement.setObject(index++, placeholder);
            }
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnLength = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < columnLength; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object value = resultSet.getObject(columnName);
                    map.put(columnName, value);
                }
                listMapResult.add(map);
            }
            return listMapResult;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
        throw new IllegalArgumentException("No corresponding value found.");
    }

    /**
     * 查询某个表单指定位置数据: SELECT col1, col2, col3, ... FROM table WHERE col1='param1', col2='param2',... .
     * @param table 待操作的表单.
     * @param column 待查询的列 colName1, colName2, ... .
     * @param conditions 附加条件, Key: 列名, Value: 对应的值.
     * @return listMapResult, 一个map的list集合, map中Key为列名,Value为对应值.
     * @see SQLException
     */
    public List<Map<String, Object>> getSpecifiedLocationData(String table, List<String> column, Map<String, Object> conditions)  {
        List<Map<String, Object>> listMapResult = new ArrayList<>();
        Iterator iter_cond = conditions.entrySet().iterator();
        StringBuilder colSet = new StringBuilder();
        StringBuilder cond = new StringBuilder();

        for (String colName : column) {
            colSet.append(",").append(colName);
        }
        colSet = new StringBuilder(colSet.substring(1));  // 去除首个","

        while (iter_cond.hasNext()) {
            Map.Entry entry = (Map.Entry) iter_cond.next();
            String conKey = entry.getKey().toString();
            Object condValue = entry.getValue().toString();
            cond.append(" AND ").append(conKey).append("='").append(condValue).append("'");
        }
        cond = new StringBuilder(cond.substring(5));  // 去除首个" AND "

        String sql = "SELECT " + colSet + " FROM " + table + " WHERE " + cond;
        System.out.println(sql);
        conn = createConnection();
        try {
            preparedStatement = conn.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnLength = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < columnLength; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object value = resultSet.getObject(columnName);
                    map.put(columnName, value);
                }
                listMapResult.add(map);
            }
            return listMapResult;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
        throw new IllegalArgumentException("No corresponding value found.");
    }

    /**
     * 查询出某个表单指定列数据: SELECT col1, col2, col3, ... FROM table.
     * @param table 待操作的表单.
     * @param column 待查询的列 colName1, colName2, ... .
     * @return listMapResult, 一个map的list集合, map中Key为列名,Value为对应值.
     * @see SQLException
     */
    public List<Map<String, Object>> getSpecifiedColumnData(String table, List<String> column)  {
        List<Map<String, Object>> listMapResult = new ArrayList<>();
        StringBuilder colSet = new StringBuilder();
        for (String colName : column) {
            colSet.append(",").append(colName);
        }
        colSet = new StringBuilder(colSet.substring(1));  // 去除首个","

        String sql = "SELECT " + colSet + " FROM " + table;
        System.out.println(sql);
        conn = createConnection();
        try {
            preparedStatement = conn.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnLength = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < columnLength; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object value = resultSet.getObject(columnName);
                    map.put(columnName, value);
                }
                listMapResult.add(map);
            }
            return listMapResult;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
        throw new IllegalArgumentException("No corresponding value found.");
    }

    /**
     * 查询出某个表单全部数据: SELECT * FROM table.
     * @param table 待操作的表单.
     * @return listMapResult, 一个map的list集合, map中Key为列名,Value为对应值.
     * @see SQLException
     */
    public List<Map<String, Object>> getAllData(String table)  {
        List<Map<String, Object>> listMapResult = new ArrayList<>();

        String sql = "SELECT * FROM " + table;
        System.out.println(sql);
        conn = createConnection();
        try {
            preparedStatement = conn.prepareStatement(sql);
            resultSet = preparedStatement.executeQuery();
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnLength = metaData.getColumnCount();
            while (resultSet.next()) {
                Map<String, Object> map = new HashMap<>();
                for (int i = 0; i < columnLength; i++) {
                    String columnName = metaData.getColumnName(i + 1);
                    Object value = resultSet.getObject(columnName);
                    map.put(columnName, value);
                }
                listMapResult.add(map);
            }
            return listMapResult;
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
        throw new IllegalArgumentException("No corresponding value found.");
    }

    /**
     * 通过SQL语句更新某个表单: UPDATE table SET col1='?', col2=?, ...
     * @param sql SQL更新语句.
     * @param params 占位符列表, param1, param2, param3,... .
     * @see SQLException
     */
    public void updateDataBySQL(String sql, List<Object> params) {
        int index = 1;  // 占位符索引
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            for (Object placeholder : params) {
                preparedStatement.setObject(index++, placeholder);
            }
            preparedStatement.executeUpdate();
            System.out.println("Table updated successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }

    /**
     * 更新某表单指定列数据: UPDATE table SET col1='param1', col2='param2', ...
     * @param table 待操作的表单.
     * @param column 待更新的列, Key: 更新的列名, Value: 用于替换的新值.
     * @see SQLException
     */
    public void updateMultiColumnData(String table, Map<String, Object> column) {
        Iterator iter_col = column.entrySet().iterator();
        StringBuilder colSet = new StringBuilder();

        while (iter_col.hasNext()) {
            Map.Entry entry = (Map.Entry) iter_col.next();
            String colKey = entry.getKey().toString();
            Object colValue = entry.getValue();
            colSet.append(",").append(colKey).append("='").append(colValue).append("'");
        }
        colSet = new StringBuilder(colSet.substring(1));  // 去除首个","

        // 获取系统当前时间
        Date now = new Date();
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String sql = "UPDATE " + table + " SET " + colSet + ",modifiedTime='" + time.format(now) + "'";
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();
            System.out.println("Table updated successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }

    /**
     * 更新某表单指定位置数据: UPDATE table SET col1='param1',col2='param2', ... WHERE col1='param1', ...
     * @param table 待操作的表单.
     * @param column 待更新的列, Key: 更新的列名, Value: 用于替换的新值.
     * @param conditions 附加条件, Key: 作为条件的列名, Value: 对应条件值.
     * @see SQLException
     */
    public void updateSpecifiedLocationData(String table, Map<String, Object> column, Map<String, Object> conditions) {
        Iterator iter_col = column.entrySet().iterator();
        Iterator iter_cond = conditions.entrySet().iterator();
        StringBuilder colSet = new StringBuilder();
        StringBuilder cond = new StringBuilder();

        while (iter_col.hasNext()) {
            Map.Entry entry = (Map.Entry) iter_col.next();
            String colKey = entry.getKey().toString();
            Object colValue = entry.getValue();
            colSet.append(",").append(colKey).append("='").append(colValue).append("'");
        }
        colSet = new StringBuilder(colSet.substring(1));  // 去除首个","
        while (iter_cond.hasNext()) {
            Map.Entry entry = (Map.Entry) iter_cond.next();
            String conKey = entry.getKey().toString();
            Object condValue = entry.getValue().toString();
            cond.append(" AND ").append(conKey).append("='").append(condValue).append("'");
        }
        cond = new StringBuilder(cond.substring(5));  // 去除首个" AND "

        // 获取系统当前时间
        Date now = new Date();
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String sql = "UPDATE " + table + " SET " + colSet + ",modifiedtime='" + time.format(now) + "'" + " WHERE " + cond;
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();
            System.out.println("Table updated successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }

    /**
     * 通过SQL语句删除数据: DELETE FROM table WHERE col1='?', col2=?, ...
     * @param sql SQL删除语句.
     * @param params 占位符列表, param1, param2, param3,... .
     * @see SQLException
     */
    public void deleteDataBySQL(String sql, List<Object> params) {
        int index = 1;  // 占位符索引
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            for (Object placeholder : params) {
                preparedStatement.setObject(index++, placeholder);
            }
            preparedStatement.executeUpdate();
            System.out.println("Records deleted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }

    /**
     * 删除某表单指定列数据: DELETE FROM table WHERE col1='param1' AND col2='param2' AND ...
     * @param table 待操作的表单.
     * @param conditions 附加条件, Key: 作为条件的列名, Value: 对应条件值.
     * @see SQLException
     */
    public void deleteSpecifiedColumnData(String table, Map<String, Object> conditions) {
        Iterator iter = conditions.entrySet().iterator();
        StringBuilder cond = new StringBuilder();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            cond.append(" AND ").append(key).append("='").append(value).append("'");
        }
        cond = new StringBuilder(cond.substring(5));  // 去除首个" AND "

        String sql = "DELETE FROM " + table + " WHERE " + cond;
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();
            System.out.println("Records deleted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }

    /**
     * 删除某个表单全部数据: DELETE FROM table;
     * @param table 待操作的表单.
     * @see SQLException
     */
    public void deleteAllData(String table) {
        String sql = "DELETE FROM " + table;
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();
            System.out.println("All Records deleted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }

    /**
     * 通过SQL语句插入数据: INSERT INTO table (col1, col2, ... ) VALUES (?, ?, ...) ...
     * @param sql SQL删除语句.
     * @param params 占位符列表, param1, param2, param3,... .
     * @see SQLException
     */
    public void insertDataBySQL(String sql, List<Object> params) {
        int index = 1;  // 占位符索引
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            for (Object placeholder : params) {
                preparedStatement.setObject(index++, placeholder);
            }
            preparedStatement.executeUpdate();
            System.out.println("Record inserted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }

    /**
     * 向某表单的指定列中插入一行数据: INSERT INTO table (col1, col2, ...) VALUES ('param1', 'param2', ...)
     * @param table 待操作的表单.
     * @param column 待插入的列, Key: 待插入的列名, Value:待插入的新值.
     * @see SQLException
     */
    public void insertOneRecordIntoSpecifiedColumn(String table, Map<String, Object> column) {
        Iterator iter = column.entrySet().iterator();
        StringBuilder colName = new StringBuilder();
        StringBuilder colValue = new StringBuilder();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String key = entry.getKey().toString();
            Object value = entry.getValue();
            colName.append(",").append(key);
            colValue.append(",'").append(value).append("'");
        }
        colName = new StringBuilder(colName.substring(1));  // 去除首个","
        colValue = new StringBuilder(colValue.substring(1));

        // 获取系统当前时间
        Date now = new Date();
        SimpleDateFormat time = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String sql = "INSERT INTO " + table +" ("+ colName + ",createtime,modifiedtime) " +
                     "VALUES (" + colValue + ",'"+ time.format(now)+ "','"+ time.format(now) + "')";
        System.out.println(sql);
        conn = createConnection();
        try{
            preparedStatement = conn.prepareStatement(sql);
            preparedStatement.executeUpdate();
            System.out.println("Record inserted successfully.");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            releaseConnection(conn, statement, preparedStatement, resultSet);
        }
    }
}
