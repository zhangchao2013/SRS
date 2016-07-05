package dbHelperTest;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.ResultSet;  
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;  
import java.util.Date;  
import java.util.HashMap;  
import java.util.List;  
import java.util.Map;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverManagerConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.dbutils.ResultSetHandler;  
import org.apache.commons.logging.Log;  
import org.apache.commons.logging.LogFactory;
import org.apache.tomcat.dbcp.pool2.ObjectPool;
import org.apache.tomcat.dbcp.pool2.impl.GenericObjectPool;
import org.junit.Test;

import util.dataAccess.dbHelper;

public class helperTest {

	private static final Log log = LogFactory.getLog(dbHelper.class);  
    
//	@Test
	public void testConnection(){
		
		@SuppressWarnings("resource")
		BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.sqlite.JDBC");
        ds.setUrl("jdbc:sqlite:db/SRSDB.sqlite");
		
        try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {

            stmt.execute("CREATE TABLE sample (id int, name varchar(255))");
           
           
        } catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
//	@Test
    public void testSinglton(){   
        Thread t1 =  new Thread(new Runnable(){   
           @Override  
           public void run() {   
               dbHelper helper1 = new dbHelper();  
           }  
               
         });   
          
        Thread t2 =  new Thread(new Runnable(){   
           @Override  
           public void run() {   
               dbHelper  helper2 = new dbHelper();  
           }  
                   
       });  
        t1.start();t2.start();  
        System.out.println("a");  
    }  
    
//    @Test
    public void testQuerySqlRsh(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEventById");  
            Event event = helper.query(sql, new ResultSetHandler<Event>(){   
                public Event handle(ResultSet rs) throws SQLException{  
                    if(rs.next()){  
                        Event event = new Event();  
                        event.setId(rs.getLong(1));  
                        event.setDate(rs.getDate(2));  
                        event.setTitle(rs.getString(3));  
                        return event;  
                    }  
                    return null;  
                }  
            },Long.valueOf(140));  
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testUpdateSql(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("updateEvent");  
            Object[] params = new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(140)};  
            int effect = helper.update(sql,params);  
            System.out.println(effect);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testBatch(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("updateEvent");  
            Object[][] params = new Object[][]{  
                    new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(140)},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(165)}           
            };  
            int effect[] = helper.batch(sql,params);  
            System.out.println(Arrays.toString(effect));  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testBatchInBatchSize(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("updateEvent");  
            Object[][] params = new Object[][]{  
                    new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(140)},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(165)},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(162)},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(163)},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"updateEvent Title",Long.valueOf(164)}  
            };  
            int effect[] = helper.batch(sql,params,2);  
            System.out.println(Arrays.toString(effect));  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
    
    @Test  
    public void testInsert(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("insertEvent");  
            Object[] params = new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"};  
            int id = helper.insert(sql,params);  
            System.out.println(id);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
  
    public void testInsertBatch(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("insertEvent");  
            String sequnceName = helper.getSql("eventsSequnce");  
            Object[][] params = new Object[][]{  
                    new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"}         
            };  
            Long effect[] = helper.insertBatch(sql,sequnceName,params);  
            System.out.println(Arrays.toString(effect));  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testInsertInBatchSize(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("insertEvent");  
            String sequnceName = helper.getSql("eventsSequnce");  
            Object[][] params = new Object[][]{  
                    new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"},  
                    new Object[]{new java.sql.Date(new Date().getTime()),"insertEvent Title"}  
            };  
            Long effect[] = helper.insertBatch(sql,sequnceName,params,2);  
            System.out.println(Arrays.toString(effect));  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testQueryBean(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEventById");  
            Event event = helper.queryBean(sql, Event.class,Long.valueOf(140));   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testQueryBeanByRenameLable(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEventById");  
            sql = "SELECT event_id id,event_date,title FROM events WHERE event_id = ?";  
            Event event = helper.queryBean(sql, Event.class,Long.valueOf(140));   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testQueryBeanByRenameMapping(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEventById");  
            Map<String,String> ctp = new HashMap<String,String>();  
            ctp.put("EVENT_ID", "id");  
            ctp.put("EVENT_DATE", "date");  
            Event event = helper.queryBean(sql, Event.class,ctp,Long.valueOf(140));   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testQueryBeanList(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");  
            Map<String,String> ctp = new HashMap<String,String>();  
            ctp.put("EVENT_ID", "id");  
            ctp.put("EVENT_DATE", "date");  
            List<Event> event = helper.queryBeanList(sql, Event.class,ctp);   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testBeanMap(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");  
            Map<String,String> ctp = new HashMap<String,String>();  
            ctp.put("EVENT_ID", "id");  
            ctp.put("EVENT_DATE", "date");  
            Map<String,Event> event = helper.queryBeanMap(sql, Event.class,"EVENT_ID",ctp);   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testQueryMap(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");   
            Map<String,Object> event = helper.queryMap(sql);   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testQueryKeyMap(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");   
           Map<Object,Map<String,Object>> event = helper.queryKeyMap(sql, "EVENT_ID");   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void testQueryListMap(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");   
            List<Map<String,Object>> event = helper.queryListMap(sql);   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void queryArray(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");   
            Object[] event = helper.queryArray(sql);   
            System.out.println(Arrays.toString(event));  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void queryArrayList(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");   
            List<Object[]> event = helper.queryArrayList(sql);   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void queryColumnList(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");   
            List<Object> event = helper.queryColumnList(sql, "EVENT_ID");   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void queryForObject(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryCounts");   
            Long event = helper.queryForObject(sql,Long.class);   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  
      
    public void queryColumn(){  
        dbHelper helper = new dbHelper("/sql/Event.xml");  
        try {  
            String sql = helper.getSql("queryEvents");   
            Object event = helper.queryColumn(sql, "EVENT_ID");   
            System.out.println(event);  
       } catch (SQLException e) {  
           log.error("sql error", e);  
       }  
    }  

}
