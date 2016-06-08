package com.kakao.adt.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import org.junit.Assert;

import com.kakao.adt.test.tool.DMLQueryTool;
import com.kakao.adt.test.tool.DMLQueryType;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import static com.kakao.adt.test.TestUtil.DB_TEST_SCHEMA;
import static com.kakao.adt.test.tool.DMLQueryTool.*;

public class TestUtil {

    public static final String DB_HOST = "adt-test-my001.example.com";
    public static final int DB_PORT = 3306;
    public static final String DB_URL = "jdbc:mysql://" + DB_HOST + ":" + DB_PORT;
    public static final String DB_USER = "adt";
    public static final String DB_PW = "adt";
    public static final String DB_TEST_SCHEMA;
    public static final int DB_SLAVE_SERVER_ID = (int)(Math.random() * 99999999);
    
    // dest list are used for shard rebalancing test
    public static final int SHARD_COUNT = 2;
    public static final String[] DB_DEST_URL_LIST_WITHOUT_SCHEMA = new String[]{
            "jdbc:mysql://adt-test-my002.example.com:3306/",
            "jdbc:mysql://adt-test-my003.example.com:3306/"
    };
    public static final String[] DB_DEST_SCHEMA_LIST = new String[SHARD_COUNT];
    public static final String[] DB_DEST_URL_LIST = new String[SHARD_COUNT];
    
    public static final MysqlDataSource testDataSource;
    private static DMLQueryTool queryTool = null;
    private static boolean isInitialized = false;
    static{
        
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        
        DB_TEST_SCHEMA = "adt_unittest";// + DateTimeFormatter.ofPattern("MMdd_HHmmss").format(ZonedDateTime.now());
        
        for(int i=0; i<SHARD_COUNT; i++){
            DB_DEST_SCHEMA_LIST[i] = DB_TEST_SCHEMA + "_temp_dest_" + i;
            DB_DEST_URL_LIST[i] = DB_DEST_URL_LIST_WITHOUT_SCHEMA[i] + DB_DEST_SCHEMA_LIST[i];
        }
        
        testDataSource = new MysqlDataSource();
        testDataSource.setUrl(DB_URL);
        testDataSource.setUser(DB_USER);
        testDataSource.setPassword(DB_PW);
        
    }
    
    public static synchronized void initializeTestEnv() throws Exception{
        if(isInitialized){
            return;
        }
        Connection conn = testDataSource.getConnection();
        try{
            Statement stmt = conn.createStatement();
            Assert.assertTrue(getMysqlSystemVariable(stmt, "log_bin").equalsIgnoreCase("on"));
            Assert.assertTrue(getMysqlSystemVariable(stmt, "binlog_format").equalsIgnoreCase("row"));
            
            stmt.execute("DROP DATABASE IF EXISTS " + DB_TEST_SCHEMA);
            stmt.execute("CREATE DATABASE " + DB_TEST_SCHEMA);
            stmt.execute(String.format(getResourceAsString("sql/create_table_adt_test_1.sql"), DB_TEST_SCHEMA));
            stmt.execute(String.format(getResourceAsString("sql/create_table_adt_test_2.sql"), DB_TEST_SCHEMA));
            stmt.execute(String.format(getResourceAsString("sql/create_table_adt_test_3.sql"), DB_TEST_SCHEMA));
            
            stmt.execute("RESET MASTER");
            
        }finally{
            conn.close();
        }
        
        startDMLTool(1000);
        
        isInitialized = true;
    }
    
    private static String getMysqlSystemVariable(Statement stmt, String varName) throws SQLException{
        
        ResultSet rs = stmt.executeQuery("SHOW GLOBAL VARIABLES WHERE variable_name='" + varName + "'");
        rs.next();
        return rs.getString("value");
        
    }
    
    private static void startDMLTool(long sleepTime) throws IOException, SQLException{
        if(queryTool != null){
            return;
        }
        
        final int tableCount = 2;
        
        Properties prop = new Properties();
        prop.setProperty(PROP_MSR_SRC_URL, DB_URL + "/" + DB_TEST_SCHEMA);
        prop.setProperty(PROP_MSR_SRC_USERNAME, DB_USER);
        prop.setProperty(PROP_MSR_SRC_PASSWROD, DB_PW);
        
        prop.setProperty(PROP_MSR_TOOL_DQT_THREAD_COUNT, "128");
        prop.setProperty(PROP_MSR_TOOL_DQT_DBCP2_MAX_TOTAL, "1024");
        prop.setProperty(PROP_MSR_TOOL_DQT_DBCP2_MAX_IDLE, "1024");
        prop.setProperty(PROP_MSR_TOOL_DQT_DBCP2_MIN_IDLE, "0");
        prop.setProperty(PROP_MSR_TOOL_DQT_TABLE_COUNT, tableCount + "");
        
        for(int i=0; i<tableCount; i++){
            if(i == 0){
                prop.setProperty(String.format(PROP_MSR_TOOL_DQT_MAX_NO, i), "1000");
                prop.setProperty(String.format(PROP_MSR_TOOL_DQT_MAX_SEQ, i), "1000");
                prop.setProperty(String.format(PROP_MSR_TOOL_DQT_MAX_UK, i), "1000");
            }else if(i == 1){
                prop.setProperty(String.format(PROP_MSR_TOOL_DQT_MAX_NO, i), "100000");
                prop.setProperty(String.format(PROP_MSR_TOOL_DQT_MAX_SEQ, i), "100000");
                prop.setProperty(String.format(PROP_MSR_TOOL_DQT_MAX_UK, i), "100000");
            }
            
            for(DMLQueryType queryType : DMLQueryType.values()){
                prop.setProperty(String.format(PROP_MSR_TOOL_DQT_QUERY_RATIO, i, queryType.getPropKey()), "1");
            }
        }
        
        queryTool = new DMLQueryTool(prop);
        
        try {
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    }
    
    public static String getResourceAsString(String path) throws Exception{

        InputStream is = ClassLoader.getSystemResourceAsStream(path);
        BufferedReader br = null;
        StringBuilder sb = new StringBuilder();

        String line;
        try {

            br = new BufferedReader(new InputStreamReader(is));
            while ((line = br.readLine()) != null) {
                sb.append(line).append('\n');
            }

        } finally {
            if (br != null) {
                br.close();
            }
            is.close();
        }

        return sb.toString();

    }
    
}
