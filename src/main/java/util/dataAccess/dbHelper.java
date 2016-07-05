package util.dataAccess;


import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.*;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbutils.*;
import org.apache.commons.dbutils.handlers.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/** 
 * just wrap DbUtils QueryRunner, provider simple functions to use. 
 *  
 * give chance to add action and do operator 
 *  
 * query: resolve all query problem. insert: get secuqnce to insert update: 
 * execute update with params. delete: execute delete with params batch: batch 
 * update and delete with params. 
 *  
 * NOTE: batch operator, please cause of batchSize, this will be set how many 
 * counts we commit, and do next batch. 
 *  
 * NOTE: below have expand of Connection PARAMS, if call please add. 
 *  
 * NOTE: if query large Datas, please set FecthSize, this will be improve 
 * performance. 
 *  
 * we often use Bean to setter/getter, with columnToPropertyOverrides,we use 
 * this to process column name not same as property. or we can do below SELECT 
 * event_id id, event_date date,title from events. this will be re-name column 
 * lable name.(care of key of Database) 
 *  
 * queryBean: query one row to Bean queryBeanList: query multiple rows to 
 * List<Bean> queryBeanMap: query multiple rows to Map<String,Bean> 
 *  
 * Query List application 
 *  
 * queryColumn: query one column value queryColumnSqlKey: query one column value 
 * with sql key queryMap: query one row to Map<String,Object> queryKeyMap: query 
 * multiple rows to Map<Object,Map<String,Object>> queryListMap: query multiple 
 * rows to List<Map<String,Object>> queryArray: query one row to Object[] 
 * queryArrayList: query multiple rows to List<Object[]> 
 *  
 *  
 *  
 * Example one: easy to queryBean. 
 *  
 * String sql = "SELECT * FROM EVENTS WHERE EVENT_ID = ?"; Object[] params = new 
 * Object[] { 130L }; Event event = helper.queryBean(sql, Event.class, params); 
 * //Event event = helper.queryBean(sql, Event.class,130L); 
 *  
 * Example two: Column not match property 
 *  
 * Map<String, String> columnToPropertyOverrides = new HashMap<String,String>(); 
 * columnToPropertyOverrides.put("EVENT_ID", "id"); 
 * columnToPropertyOverrides.put("EVENT_DATE", "date"); String sql = 
 * "SELECT * FROM EVENTS WHERE EVENT_ID = ?"; Object[] params = new Object[] { 
 * 130L }; Event event = helper.queryBean(sql, 
 * Event.class,columnToPropertyOverrides, params); 
 *  
 * Example three: easy to insert 
 *  
 * String sql = "INSERT into events(event_id,event_date,title) VALUES(?,?,?)"; 
 * Object[] params = new Object[] { event.getDate(),event.getTitle() }; Long id 
 * = helper.insert(sql, "events_sequnce", params); 
 *  
 * Example four: easy to query one column String sql = 
 * "SELECT count(*) eventCount  FROM events"; Object count = 
 * helper.queryColumn(sql, "eventCount"); 
 *  
 * Example five: easy to get one column multiple rows List<ID> 
 *  
 * String sql = "SELECT event_id  FROM events"; List<Object> count = 
 * helper.queryColumnList(sql, "event_id"); 
 *  
 * Example six: easy to do it in batch 
 *  
 * String sql = 
 * "UPDATE events SET event_date = ?, title = ? WHERE event_id = ?"; Object[][] 
 * params = new Object[events.size()][]; for(int i=0 i< events.size();i++){ 
 * Event event = events.get(i); params[i] = new Object[] { 
 * event.getDate(),event.getTitle(),event.getId()}; } int[] effectrows = 
 * helper.batch(sql, params); 
 *  
 * Example seven: easy to do insert batch, return all ids. 
 *  
 * String sql = "INSERT into events(event_id,event_date,title) VALUES(?,?,?)"; 
 * Object[][] params = new Object[events.size()][]; for(int i=0 i< 
 * events.size();i++){ Event event = events.get(i); params[i] = new Object[] { 
 * event.getDate(),event.getTitle()}; } Long[] ids = 
 * helper.insertBatch(sql,"events_sequnce" ,params); 
 *  
 * @author
 *  
 */  
public class dbHelper {
	
	private static final Log log = LogFactory.getLog(dbHelper.class);
	  
    private DataSource dataSource = null;  
    private static Map<String,DataSource> cachds = new ConcurrentHashMap<String,DataSource>();   
    private QueryRunner queryRunner = null;  
    private Map<String, String> sqlKeys = new HashMap<String, String>();  
  
    public dbHelper() {  
        this(null, null);  
    }  
  
    public dbHelper(String path) {  
        this(null, path);  
    }  
  
    public dbHelper(DataSource dataSource) {  
        this(dataSource, null);  
    }  
  
    public dbHelper(DataSource dataSource, String path) {  
        this(dataSource,path,0);  
    }  
      
    public dbHelper(DataSource dataSource, String path, final int fecthSize) {   
        if (dataSource == null) {  
            dataSource = ConnectionManager.getDataSource();  
        }   
        String dskey = dataSource.toString()+fecthSize;   
        DataSource ds = cachds.get(dskey);  
        if(ds == null){  
            this.dataSource = dataSource;  
            queryRunner = new QueryRunner(dataSource){  
                protected PreparedStatement prepareStatement(Connection conn, String sql)  
                        throws SQLException {  
                    PreparedStatement stmt = conn.prepareStatement(sql);  
                    if(fecthSize>0){  
                        stmt.setFetchSize(fecthSize);  
                    }  
                    return stmt;  
                }  
            };  
            cachds.put(dskey, dataSource);  
        }   
          
        if (path != null) {  
            sqlKeys = loadQueries(path);  
        }  
    }    
   
	/** 
     * Executes the given SELECT SQL without any replacement parameters. The 
     * <code>Connection</code> is retrieved from the <code>DataSource</code> set 
     * in the constructor. 
     *  
     * @param <T> 
     *            The type of object that the handler returns 
     * @param sql 
     *            The SQL statement to execute. 
     * @param rsh 
     *            The handler used to create the result object from the 
     *            <code>ResultSet</code>. 
     *  
     * @return An object generated by the handler. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public <T> T query(String sql, ResultSetHandler<T> rsh) throws SQLException {  
        return queryRunner.query(sql, rsh);  
    }  
  
    /** 
     * Executes the given SELECT SQL query and returns a result object. The 
     * <code>Connection</code> is retrieved from the <code>DataSource</code> set 
     * in the constructor. 
     *  
     * @param <T> 
     *            The type of object that the handler returns 
     * @param sql 
     *            The SQL statement to execute. 
     * @param rsh 
     *            The handler used to create the result object from the 
     *            <code>ResultSet</code>. 
     * @param params 
     *            Initialize the PreparedStatement's IN parameters with this 
     *            array. 
     * @return An object generated by the handler. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public <T> T query(String sql, ResultSetHandler<T> rsh, Object... params)  
            throws SQLException {  
        return queryRunner.query(sql, rsh, params);  
    }  
  
    /** 
     * Execute an SQL SELECT query without any replacement parameters. The 
     * caller is responsible for closing the connection. 
     *  
     * @param <T> 
     *            The type of object that the handler returns 
     * @param conn 
     *            The connection to execute the query in. 
     * @param sql 
     *            The query to execute. 
     * @param rsh 
     *            The handler that converts the results into an object. 
     * @return The object returned by the handler. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh)  
            throws SQLException {  
        return queryRunner.query(conn, sql, rsh);  
    }  
  
    /** 
     * Execute an SQL SELECT query with replacement parameters. The caller is 
     * responsible for closing the connection. 
     *  
     * @param <T> 
     *            The type of object that the handler returns 
     * @param conn 
     *            The connection to execute the query in. 
     * @param sql 
     *            The query to execute. 
     * @param rsh 
     *            The handler that converts the results into an object. 
     * @param params 
     *            The replacement parameters. 
     * @return The object returned by the handler. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public <T> T query(Connection conn, String sql, ResultSetHandler<T> rsh,  
            Object... params) throws SQLException {  
        return queryRunner.query(conn, sql, rsh, params);  
    }  
  
    /** 
     * Executes the given INSERT, UPDATE, or DELETE SQL statement without any 
     * replacement parameters. The <code>Connection</code> is retrieved from the 
     * <code>DataSource</code> set in the constructor. This 
     * <code>Connection</code> must be in auto-commit mode or the update will 
     * not be saved. 
     *  
     * @param sql 
     *            The SQL statement to execute. 
     * @throws SQLException 
     *             if a database access error occurs 
     * @return The number of rows updated. 
     */  
    public int update(String sql) throws SQLException {  
        return queryRunner.update(sql);  
    }  
  
    /** 
     * Executes the given INSERT, UPDATE, or DELETE SQL statement. The 
     * <code>Connection</code> is retrieved from the <code>DataSource</code> set 
     * in the constructor. This <code>Connection</code> must be in auto-commit 
     * mode or the update will not be saved. 
     *  
     * @param sql 
     *            The SQL statement to execute. 
     * @param params 
     *            Initializes the PreparedStatement's IN (i.e. '?') parameters. 
     * @throws SQLException 
     *             if a database access error occurs 
     * @return The number of rows updated. 
     */  
    public int update(String sql, Object... params) throws SQLException {  
        return queryRunner.update(sql, params);  
    }  
  
    /** 
     * Execute an SQL INSERT, UPDATE, or DELETE query without replacement 
     * parameters. 
     *  
     * @param conn 
     *            The connection to use to run the query. 
     * @param sql 
     *            The SQL to execute. 
     * @return The number of rows updated. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public int update(Connection conn, String sql) throws SQLException {  
        return queryRunner.update(conn, sql);  
    }  
  
    /** 
     * Execute an SQL INSERT, UPDATE, or DELETE query. 
     *  
     * @param conn 
     *            The connection to use to run the query. 
     * @param sql 
     *            The SQL to execute. 
     * @param params 
     *            The query replacement parameters. 
     * @return The number of rows updated. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public int update(Connection conn, String sql, Object... params)  
            throws SQLException {  
        return queryRunner.update(conn, sql, params);  
    }  
  
    /** 
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries. The 
     * <code>Connection</code> is retrieved from the <code>DataSource</code> set 
     * in the constructor. This <code>Connection</code> must be in auto-commit 
     * mode or the update will not be saved. 
     *  
     * @param sql 
     *            The SQL to execute. 
     * @param params 
     *            An array of query replacement parameters. Each row in this 
     *            array is one set of batch replacement values. 
     * @return The number of rows updated per statement. 
     * @throws SQLException 
     *             if a database access error occurs 
     * @since DbUtils 1.1 
     */  
    public int[] batch(String sql, Object[][] params) throws SQLException {  
        return queryRunner.batch(sql, params);  
    }  
  
    /** 
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries. 
     *  
     * @param conn 
     *            The Connection to use to run the query. The caller is 
     *            responsible for closing this Connection. 
     * @param sql 
     *            The SQL to execute. 
     * @param params 
     *            An array of query replacement parameters. Each row in this 
     *            array is one set of batch replacement values. 
     * @return The number of rows updated per statement. 
     * @throws SQLException 
     *             if a database access error occurs 
     * @since DbUtils 1.1 
     */  
    public int[] batch(Connection conn, String sql, Object[][] params)  
            throws SQLException {  
        return queryRunner.batch(conn, sql, params);  
    }  
  
    /** 
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries, use batchSize to execute. 
     *  
     * @param sql 
     *            The SQL to execute. 
     * @param params 
     *            An array of query replacement parameters. Each row in this 
     *            array is one set of batch replacement values. 
     * @param batchSize 
     *            how many rows we should execute once 
     * @return The number of rows updated per statement. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public int[] batch(String sql, Object[][] params, int batchSize)  
            throws SQLException {  
        Connection conn = dataSource.getConnection();  
        int[] effects = new int[params.length];  
        if (supportsBatchUpdates(conn)) {  
            int n = 0;  
            int batchIndex = 0;  
            Object[][] batch = new Object[batchSize][];  
            for (Object[] paramsc : params) {  
                batch[n++ % batchSize] = paramsc;  
                if (n % batchSize == 0 || n == params.length) {  
                    int batchIdx = (n % batchSize == 0) ? n / batchSize  
                            : (n / batchSize) + 1;  
                    int items = n  
                            - ((n % batchSize == 0) ? n / batchSize - 1  
                                    : (n / batchSize)) * batchSize;  
                    log.debug("Sending SQL batch update #" + batchIdx  
                            + " with " + items + " items");  
                    if (n % batchSize != 0) {// batch is less then batchSize.  
                        batch = Arrays.copyOf(batch, n % batchSize);  
                    }  
                    int[] effectbatchs = batch(conn, sql, batch);  
                    for (int effectbatch : effectbatchs) {  
                        effects[batchIndex++] = effectbatch;  
                    }  
                    // after process, clear  
                    batch = new Object[batchSize][];  
                }  
  
            }  
        } else {  
            int index = 0;  
            for (Object[] paramsc : params) {  
                effects[index++] = update(conn, sql, paramsc);  
            }  
        }  
        conn.close();  
        return effects;  
    }  
  
    /** 
     * Execute a batch of SQL INSERT, UPDATE, or DELETE queries, use batchSize to execute. 
     *  
     * @param conn 
     *            The Connection to use to run the query. The caller is 
     *            responsible for closing this Connection. 
     * @param sql 
     *            The SQL to execute. 
     * @param params 
     *            An array of query replacement parameters. Each row in this 
     *            array is one set of batch replacement values. 
     * @param batchSize 
     *            how many rows we should execute once 
     * @return The number of rows updated per statement. 
     * @throws SQLException 
     *             if a database access error occurs 
     */  
    public int[] batch(Connection conn, String sql, Object[][] params,  
            int batchSize) throws SQLException {  
        if (supportsBatchUpdates(conn)) {  
            int n = 0;  
            Object[][] batch = new Object[batchSize][];  
            int[] effects = new int[params.length];  
            for (Object[] paramsc : params) {  
                batch[n++ % batchSize] = paramsc;  
                if (n % batchSize == 0 || n == params.length) {  
                    int batchIdx = (n % batchSize == 0) ? n / batchSize  
                            : (n / batchSize) + 1;  
                    int items = n  
                            - ((n % batchSize == 0) ? n / batchSize - 1  
                                    : (n / batchSize)) * batchSize;  
                    log.debug("Sending SQL batch update #" + batchIdx  
                            + " with " + items + " items");  
                    int[] effectbatchs = batch(conn, sql, batch);  
                    for (int effectbatch : effectbatchs) {  
                        effects[batchIdx * batchSize + n] = effectbatch;  
                    }  
                }  
            }  
            return effects;  
        } else {  
            int[] effects = new int[params.length];  
            int index = 0;  
            for (Object[] paramsc : params) {  
                effects[index++] = update(conn, sql, paramsc);  
            }  
            return effects;  
        }  
    }    
    
    public int insert(String sql, Object... params)  
            throws SQLException {  
    	 int Id = 0;
    	 Object[] paramsCopy = new Object[params.length + 1];  
         paramsCopy[0] = Id;  
         int index = 1;  
         for (Object param : params) {  
             paramsCopy[index++] = param;  
         }  
   
         int effect = update(sql, paramsCopy);  
         if (effect != 0) {// insert success  
             return Id;  
         } else {  
             throw new SQLException("Insert fail, please check params[" + sql  + "]");  
         } 
    }  
    
    /** 
     * Oracle insert, give sequnceName to insert. 
     *    1.  to get next SequnceId, then we all use it as Long. 
     *    2.  add this sequnceId to params[0], add all others to index++. 
     *    3.  Execute an SQL INSERT, and return effect row. 
     *    4.  if effect row is not 0, then success, or exception. 
     *   
     * @param sql 
     * @param sequnceName 
     * @param params 
     * @return 
     * @throws SQLException 
     */  
    public Long insert(String sql, String sequnceName, Object... params)  
            throws SQLException {  
        Connection conn = dataSource.getConnection();  
        Long id = insert(conn, sql, sequnceName, params);  
        conn.close();  
        return id;  
    }  
      
    /** 
     * Oracle insert, give sequnceName to insert. 
     *    1.  to get next SequnceId, then we all use it as Long. 
     *    2.  add this sequnceId to params[0], add all others to index++. 
     *    3.  Execute an SQL INSERT, and return effect row. 
     *    4.  if effect row is not 0, then success, or exception. 
     *  
     * @param conn 
     * @param sql 
     * @param sequnceName 
     * @param params 
     * @return 
     * @throws SQLException 
     */  
    public Long insert(Connection conn, String sql, String sequnceName,  
            Object... params) throws SQLException {  
        // get sequnce id  
        Long sequnceId = null;  
        String sequnceSql = getSequnceSql(sequnceName);  
        ScalarHandler<Object> rsh = new ScalarHandler<Object>();  
        Object keyId = query(conn, sequnceSql, rsh);  
        if (Number.class.isAssignableFrom(keyId.getClass())) {  
            sequnceId = Long.valueOf(((Number) keyId).longValue());  
        }  
  
        // add params to first of the params  
        Object[] paramsCopy = new Object[params.length + 1];  
        paramsCopy[0] = sequnceId;  
        int index = 1;  
        for (Object param : params) {  
            paramsCopy[index++] = param;  
        }  
  
        int effect = update(conn, sql, paramsCopy);  
        if (effect != 0) {// insert success  
            return sequnceId;  
        } else {  
            throw new SQLException("Insert fail, please check params[" + sql  
                    + "," + sequnceName + "," + params + "]");  
        }  
    }  
  
    /** 
     * Oracle insert Batch, give sequnceName to insert.(Batch) 
     *    1.  to get next SequnceId, then we all use it as Long. 
     *    2.  add this sequnceId to params[0], add all others to index++. 
     *    3.  Execute an SQL INSERT, and return effect row. 
     *    4.  if effect row is not 0, then success, or exception. 
     *  
     * @param conn 
     * @param sql 
     * @param sequnceName 
     * @param params 
     * @return 
     * @throws SQLException 
     */  
    public Long[] insertBatch(String sql, String sequnceName, Object params[][])  
            throws SQLException {  
        Connection conn = dataSource.getConnection();  
        Long[] id = insertBatch(conn, sql, sequnceName, params);  
        conn.close();  
        return id;  
    }  
           
  
    /** 
     * Oracle insert Batch, give sequnceName to insert.(Batch) 
     *  
     * @param conn 
     * @param sql 
     * @param sequnceName 
     * @param paramAlls 
     * @return 
     * @throws SQLException 
     */  
    public Long[] insertBatch(Connection conn, String sql, String sequnceName,  
            Object[][] paramAlls) throws SQLException {  
        Object[][] parambatchs = new Object[paramAlls.length][];  
        Long[] ids = new Long[paramAlls.length];  
        for (int i = 0; i < paramAlls.length; i++) {  
            Object[] paramAll = paramAlls[i];  
            // get sequnce id  
            Long id = null;  
            String sequnceSql = getSequnceSql(sequnceName);  
            ScalarHandler<Object> rsh = new ScalarHandler<Object>();  
            Object keyId = query(conn, sequnceSql, rsh);  
            if (Number.class.isAssignableFrom(keyId.getClass())) {  
                id = Long.valueOf(((Number) keyId).longValue());  
                ids[i] = id;  
            }  
  
            // add params to first of the params  
            Object[] paramsCopy = new Object[paramAll.length + 1];  
            paramsCopy[0] = id;  
            int index = 1;  
            for (Object param : paramAll) {  
                paramsCopy[index++] = param;  
            }  
            parambatchs[i] = paramsCopy;  
        }  
  
        batch(conn, sql, parambatchs);  
  
        return ids;  
    }  
      
  
    /** 
     * Oracle insert Batch, give sequnceName to insert.(Batch) 
     *    1.  to get next SequnceId, then we all use it as Long. 
     *    2.  add this sequnceId to params[0], add all others to index++. 
     *    3.  Execute an SQL INSERT, and return effect row. 
     *    4.  if effect row is not 0, then success, or exception. 
     *  
     * @param conn 
     * @param sql 
     * @param sequnceName 
     * @param params 
     * @return 
     * @throws SQLException 
     */  
    public Long[] insertBatch(String sql, String sequnceName, Object params[][],int batchSize) throws SQLException {  
        Connection conn = dataSource.getConnection();  
        Long[] id = insertBatch(conn, sql, sequnceName, params,batchSize);  
        conn.close();  
        return id;  
    }  
      
    /** 
     * Oracle insert Batch, give sequnceName to insert.(Batch) 
     *  
     * @param conn 
     * @param sql 
     * @param sequnceName 
     * @param paramAlls 
     * @return 
     * @throws SQLException 
     */  
    public Long[] insertBatch(Connection conn, String sql, String sequnceName,  
            Object[][] paramAlls,int batchSize) throws SQLException {  
          
        if (supportsBatchUpdates(conn)) {  
            int n = 0;  
            Object[][] batch = new Object[batchSize][];  
            Long[] ids = new Long[paramAlls.length];  
            int batchIndex = 0;  
            for (Object[] paramsc : paramAlls) {  
                batch[n++ % batchSize] = paramsc;  
                if (n % batchSize == 0 || n == paramAlls.length) {  
                    int batchIdx = (n % batchSize == 0) ? n / batchSize  
                            : (n / batchSize) + 1;  
                    int items = n  
                            - ((n % batchSize == 0) ? n / batchSize - 1  
                                    : (n / batchSize)) * batchSize;  
                    log.debug("Sending SQL batch insert #" + batchIdx  
                            + " with " + items + " items");  
                    if (n % batchSize != 0) {// batch is less then batchSize.  
                        batch = Arrays.copyOf(batch, n % batchSize);  
                    }  
                    Long[] idsubs = insertBatch(conn, sql,sequnceName, batch);  
                    for (Long idsub : idsubs) {  
                        ids[batchIndex++] = idsub;  
                    }  
                    // after process, clear  
                    batch = new Object[batchSize][];  
                }  
            }  
            return ids;  
        } else {  
            Long[] ids = new Long[paramAlls.length];  
            int index = 0;  
            for (Object[] paramsc : paramAlls) {  
                ids[index++] = insert(conn, sql,sequnceName, paramsc);  
            }  
            return ids;  
        }    
    }  
   
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Bean] 
     *  
     * @param sql 
     *            query SQL 
     * @param clazz 
     *            target Class 
     * @return 
     * @throws SQLException 
     */  
    public <T> T queryBean(String sql, final Class<T> clazz)  
            throws SQLException {  
        return queryBean(sql, clazz, new HashMap<String, String>());  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Bean] 
     *  
     * @param columnToPropertyOverrides 
     *            column to property overrides 
     * @return 
     * @throws SQLException 
     */  
    public <T> T queryBean(String sql, final Class<T> clazz, Object... params)  
            throws SQLException {  
        return queryBean(sql, clazz, new HashMap<String, String>(), params);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Bean] 
     *  
     * @param params 
     *            SQL PARAMS 
     * @param columnToPropertyOverrides 
     *            column to property overrides 
     * @return 
     * @throws SQLException 
     */  
    public <T> T queryBean(String sql, final Class<T> clazz,  
            final Map<String, String> columnToPropertyOverrides,  
            Object... params) throws SQLException {  
        ResultSetHandler<T> rsh = new ResultSetHandler<T>() {  
            @Override  
            public T handle(ResultSet rs) throws SQLException {  
                BeanProcessor bp = new BeanProcessor(columnToPropertyOverrides);  
                if (rs.next()) {  
                    return bp.toBean(rs, clazz);  
                }  
                return null;  
            }  
        };  
        return query(sql, rsh, params);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[List<Bean>] 
     *  
     * @param sql 
     *            query SQL 
     * @param clazz 
     *            target class 
     * @return 
     * @throws SQLException 
     */  
    public <T> List<T> queryBeanList(String sql, final Class<T> clazz)  
            throws SQLException {  
        return queryBeanList(sql, clazz, new HashMap<String, String>(),  
                (Object[]) null);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[List<Bean>] 
     *  
     * @param params 
     *            SQL PARAMS 
     * @return 
     * @throws SQLException 
     */  
    public <T> List<T> queryBeanList(String sql, final Class<T> clazz,  
            Object... params) throws SQLException {  
        return queryBeanList(sql, clazz, new HashMap<String, String>(), params);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[List<Bean>] 
     *  
     * @param columnToPropertyOverrides 
     *            column to property overrides 
     * @return 
     * @throws SQLException 
     */  
    public <T> List<T> queryBeanList(String sql, final Class<T> clazz,  
            final Map<String, String> columnToPropertyOverrides,  
            Object... params) throws SQLException {  
        ResultSetHandler<List<T>> rsh = new ResultSetHandler<List<T>>() {  
            @Override  
            public List<T> handle(ResultSet rs) throws SQLException {  
                BeanProcessor bp = new BeanProcessor(columnToPropertyOverrides);  
                return bp.toBeanList(rs, clazz);  
            }  
  
        };  
        return query(sql, rsh, params);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Map<String,<Bean>>] 
     *  
     * @param sql 
     * @param clazz 
     * @param columnName 
     * @return 
     * @throws SQLException 
     */  
    public <T> Map<String, T> queryBeanMap(String sql, final Class<T> clazz,  
            String columnName) throws SQLException {  
        return queryBeanMap(sql, clazz, columnName,  
                new HashMap<String, String>(), (Object[]) null);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Map<String,<Bean>>] 
     *  
     * @param sql 
     * @param clazz 
     * @param columnName 
     * @return 
     * @throws SQLException 
     */  
    public <T> Map<String, T> queryBeanMap(String sql, final Class<T> clazz,  
            String columnName, Object... params) throws SQLException {  
        return queryBeanMap(sql, clazz, columnName,  
                new HashMap<String, String>(), params);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Map<String,<Bean>>] 
     *  
     * @param sql 
     * @param clazz 
     * @param columnName 
     * @return 
     * @throws SQLException 
     */  
    public <T> Map<String, T> queryBeanMap(String sql, final Class<T> clazz,  
            final String columnName,  
            final Map<String, String> columnToPropertyOverrides,  
            Object... params) throws SQLException {  
        ResultSetHandler<Map<String, T>> rsh = new ResultSetHandler<Map<String, T>>() {  
            BeanProcessor bp = new BeanProcessor(columnToPropertyOverrides);  
  
            @Override  
            public Map<String, T> handle(ResultSet rs) throws SQLException {  
                Map<String, T> result = new HashMap<String, T>();  
                while (rs.next()) {  
                    result.put(createKey(rs), createRow(rs));  
                }  
                return result;  
            }  
  
            private String createKey(ResultSet rs) throws SQLException {  
                return rs.getString(columnName);  
            }  
  
            private T createRow(ResultSet rs) throws SQLException {  
                return bp.toBean(rs, clazz);  
            }  
        };  
        return query(sql, rsh, params);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Map<String,<Object>>] 
     *  
     * @param sql 
     * @return 
     * @throws SQLException 
     */  
    public <T> Map<String, Object> queryMap(String sql) throws SQLException {  
        MapHandler kh = new MapHandler();  
        return query(sql, kh);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to 
     * Class<T>[Map<Object,Map<String,<Object>>>] 
     *  
     * @param sql 
     * @param column 
     * @return 
     * @throws SQLException 
     */  
    public <T> Map<Object, Map<String, Object>> queryKeyMap(String sql,  
            String column) throws SQLException {  
        KeyedHandler<Object> kh = new KeyedHandler<Object>(column);  
        return query(sql, kh);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[List<Map<String,Object>>] 
     *  
     * @param sql 
     * @return 
     * @throws SQLException 
     */  
    public <T> List<Map<String, Object>> queryListMap(String sql)  
            throws SQLException {  
        MapListHandler kh = new MapListHandler();  
        return query(sql, kh);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[Object[]] 
     *  
     * @param sql 
     * @return 
     * @throws SQLException 
     */  
    public <T> Object[] queryArray(String sql) throws SQLException {  
        ArrayHandler kh = new ArrayHandler();  
        return query(sql, kh);  
    }  
  
    /** 
     * execute SQL, transform ResultSet to Class<T>[List<Object[]>] 
     *  
     * @param sql 
     * @return 
     * @throws SQLException 
     */  
    public <T> List<Object[]> queryArrayList(String sql) throws SQLException {  
        ArrayListHandler kh = new ArrayListHandler();  
        return query(sql, kh);  
    }  
  
    /** 
     * query column name, return column values 
     *  
     * @param sql 
     *            query SQL 
     * @param columnName 
     *            column name 
     * @return 
     * @throws SQLException 
     */  
    public <T> List<T> queryColumnList(String sql, String columnName)  
            throws SQLException {  
        return queryColumnList(sql, columnName, (Object[]) null);  
    }  
  
    /** 
     * query column name, return column values 
     *  
     * @param params 
     *            SQL PARAMS 
     * @return 
     * @throws SQLException 
     */  
    public <T> List<T> queryColumnList(String sql, String columnName,  
            Object... params) throws SQLException {  
        ColumnListHandler<T> clh = new ColumnListHandler<T>(columnName);  
        return query(sql, clh, params);  
    }  
  
    /** 
     * query object, (T)rs.getObject(1), return column value 
     *  
     *  
     * @param sql 
     * @param clazz 
     * @return 
     * @throws SQLException 
     */  
    public <T> T queryForObject(String sql, Class<T> clazz) throws SQLException {  
        return queryForObject(sql, clazz, (Object[]) null);  
    }  
  
    /** 
     * query object, (T)rs.getObject(1), return column value 
     *  
     *  
     * @param sql 
     * @param clazz 
     * @return 
     * @throws SQLException 
     */  
    public <T> T queryForObject(String sql, final Class<T> clazz,  
            Object... params) throws SQLException {  
        ScalarHandler<T> kh = new ScalarHandler<T>() {  
            @SuppressWarnings("unchecked")  
            public T handle(ResultSet rs) throws SQLException {  
                T target = null;  
                Object resultObject = null;  
                if (rs.next()) {  
                    resultObject = rs.getObject(1);  
                }  
                if (resultObject != null  
                        && Number.class.isAssignableFrom(resultObject  
                                .getClass())) {  
                    if (BigDecimal.class.isInstance(resultObject)) {  
                        BigDecimal bd = (BigDecimal) resultObject;  
                        if (Integer.class.isAssignableFrom(clazz)) {  
                            target = (T) Integer.valueOf(bd.intValue());  
                        } else if (Long.class.isAssignableFrom(clazz)) {  
                            target = (T) Long.valueOf(bd.longValue());  
                        } else if (Float.class.isAssignableFrom(clazz)) {  
                            target = (T) Float.valueOf(bd.floatValue());  
                        } else if (Double.class.isAssignableFrom(clazz)) {  
                            target = (T) Double.valueOf(bd.doubleValue());  
                        }  
                    }  
                } else {  
                    target = (T) resultObject;  
                }  
                return target;  
            }  
        };  
        return query(sql, kh, params);  
    }  
  
    /** 
     * query column name, return column value 
     *  
     * @param sql 
     *            query SQL 
     * @param columnName 
     *            column name 
     * @return column value 
     * @throws SQLException 
     */  
    public <T> T queryColumn(String sql, String columnName) throws SQLException {  
        return queryColumn(sql, columnName, (Object[]) null);  
    }  
  
    /** 
     * query column name, return column value 
     *  
     * @param sql 
     *            query SQL 
     * @param columnName 
     *            column name 
     * @return column value 
     * @throws SQLException 
     */  
    public <T> T queryColumn(String sql, String columnName, Object... params)  
            throws SQLException {  
        ScalarHandler<T> kh = new ScalarHandler<T>(columnName);  
        return query(sql, kh, params);  
    }  
  
    /** 
     * Return whether the given JDBC driver supports JDBC 2.0 batch updates. 
     * <p> 
     * Typically invoked right before execution of a given set of statements: to 
     * decide whether the set of SQL statements should be executed through the 
     * JDBC 2.0 batch mechanism or simply in a traditional one-by-one fashion. 
     * <p> 
     * Logs a warning if the "supportsBatchUpdates" methods throws an exception 
     * and simply returns {@code false} in that case. 
     *  
     * @param con 
     *            the Connection to check 
     * @return whether JDBC 2.0 batch updates are supported 
     * @see java.sql.DatabaseMetaData#supportsBatchUpdates() 
     */  
    private static boolean supportsBatchUpdates(Connection con) {  
        try {  
            DatabaseMetaData dbmd = con.getMetaData();  
            if (dbmd != null) {  
                if (dbmd.supportsBatchUpdates()) {  
                    log.debug("JDBC driver supports batch updates");  
                    return true;  
                } else {  
                    log.debug("JDBC driver does not support batch updates");  
                }  
            }  
        } catch (SQLException ex) {  
            log.debug(  
                    "JDBC driver 'supportsBatchUpdates' method threw exception",  
                    ex);  
        } catch (AbstractMethodError err) {  
            log.debug(  
                    "JDBC driver does not support JDBC 2.0 'supportsBatchUpdates' method",  
                    err);  
        }  
        return false;  
    }  
  
    /** 
     * get SQL with sqlKey 
     *  
     * NOTE: you must be give path in construct 
     *  
     * @param sqlkey 
     * @return 
     * @throws SQLException 
     */  
    public String getSql(String sqlkey) throws SQLException {  
        String sql = sqlKeys.get(sqlkey);  
        return sql;  
    }  
      
    /** 
     * via sequnceName to generator SQL 
     *  
     * @param sequnceName 
     * @return 
     */  
    private String getSequnceSql(String sequnceName){  
        String sequnceSql = "select " + sequnceName  
                + ".nextval  from dual";  
        return sequnceSql;  
    }  
  
    /** 
     * load queries from path 
     *  
     * @param path 
     * @return 
     */  
    public Map<String, String> loadQueries(String path) {  
        QueryLoader ql = QueryLoader.instance();  
        Map<String, String> queries = new HashMap<String, String>();  
        try {  
            queries = ql.load(path);  
        } catch (IOException e) {  
            throw new RuntimeException("Load Queries  fail, [" + path + "]");  
        }  
        return queries;  
    }  
}   

