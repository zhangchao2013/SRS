package util.dataAccess;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

//import com.mysql.jdbc.Connection;

public class ConnectionManager {
    
//    private static final String DRIVER = "com.mysql.jdbc.Driver";
//    private static final String URL = "jdbc:mysql:///shop_for_dbutils";
//    private static final String NAME = "root";
//    private static final String PASSWORD = "root";
    
    private static final String DRIVER = "org.sqlite.JDBC";
    private static final String URL = "jdbc:sqlite:db/SRSDB.sqlite";
    private static final String NAME = null;
    private static final String PASSWORD = null;
    
    
    public static BasicDataSource dataSource = buildDataSource();

    private static BasicDataSource buildDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(DRIVER);
        ds.setUrl(URL);
//        ds.setUsername(NAME);
//        ds.setPassword(PASSWORD);
        return ds;
    }
    
    public static BasicDataSource getDataSource() {
        return dataSource;
    }
    
    public static Connection getConnection() throws SQLException {
        return (Connection) dataSource.getConnection();
    }    
}