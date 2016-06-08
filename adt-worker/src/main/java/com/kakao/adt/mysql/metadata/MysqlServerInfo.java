package com.kakao.adt.mysql.metadata;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class MysqlServerInfo {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlServerInfo.class);
    static{
        try{
            Class.forName("com.mysql.jdbc.Driver");
        }catch(Exception e){
            LOGGER.error("Failed to init mysql jdbc driver", e);
        }
    }
    
    final private DataSource ds;
    private final List<String> dbNameList;
    private List<BinaryLogFile> binaryLogFileList = new ArrayList<>();
    private Map<String, Database> databases = new HashMap<>();
    
    public MysqlServerInfo(String host, int port, String user, String password, List<String> dbNameList) throws SQLException{
        ds = new MysqlDataSource();
        ((MysqlDataSource)ds).setServerName(host);
        ((MysqlDataSource)ds).setPort(port);
        ((MysqlDataSource)ds).setUser(user);
        ((MysqlDataSource)ds).setPassword(password);
        
        this.dbNameList = dbNameList;
        
        initialize();
    }
    
    public MysqlServerInfo(String host, int port, String user, String password, List<String> dbNameList, boolean crawlBinlogFileList) throws SQLException{
        this(host, port, user, password, dbNameList);
        if(crawlBinlogFileList){
            crawlBinlogFileList();
        }
    }
    
    public void initialize() throws SQLException{
        
        for(final String dbName : dbNameList){
            Database db = new Database();
            db.setName(dbName);
            databases.put(dbName, db);
        }
        
        for(final Database database : databases.values()){
            crawlTables(database);
            for(final Table table : database.getTables()){
                crawlColumns(table);
                crawlIndexes(table);
            }
        }
        
    }
    
    public Database getDatabase(String dbName){
        return this.databases.get(dbName);
    }
    
    public Collection<Database> getDatabases(){
        return this.databases.values();
    }
    
    public BinaryLogFile getFirstBinaryLogFile(){
        return binaryLogFileList.get(0);
    }
    
    public BinaryLogFile getLastBinaryLogFile(){
        return binaryLogFileList.get(binaryLogFileList.size() - 1);
    }
    
    public List<BinaryLogFile> getBinaryLogFileList(){
        return this.binaryLogFileList;
    }
    
    private List<Map<String, String>> executeQuery(String sql) throws SQLException{
        LOGGER.debug(sql);
        Connection conn = ds.getConnection();
        try{
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(sql);
            
            final List<Map<String, String>> result = new ArrayList<>();
            final int colCount = rs.getMetaData().getColumnCount();
            while(rs.next()){
                final Map<String, String> aRow = new HashMap<>(); 
                for(int i=0; i<colCount; i++){
                    final String key = rs.getMetaData().getColumnName(i+1);
                    final String value = rs.getString(i+1);
                    aRow.put(key, value);
                }
                result.add(aRow);
            }
            
            LOGGER.debug(result.toString());
            
            return result;
        }finally{
            conn.close();
        }
        
    }
    
    public void crawlBinlogFileList() throws SQLException{
        
        final String sql = "SHOW BINARY LOGS";
        final List<Map<String, String>> sqlResult = executeQuery(sql);
        
        for(final Map<String, String> result : sqlResult){
            
            BinaryLogFile logFile = new BinaryLogFile(
                    result.get("Log_name"), 
                    Long.parseLong(result.get("File_size")));
            binaryLogFileList.add(logFile);
        }
        
    }
    
    public void crawlTables(Database db) throws SQLException{
        
        final String sql = String.format(
                "SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE " + 
                "table_schema = '%s'", 
                db.getName());
        final List<Map<String, String>> sqlResult =  executeQuery(sql);
        
        for(final Map<String, String> result : sqlResult){
            
            Table table = new Table();
            table.setDatabase(db);
            table.setName(result.get("TABLE_NAME"));
            
            db.addTable(table);
            
        }
        
    }
    
    public void crawlColumns(Table table) throws SQLException{
        
        final String sql = String.format(
                "SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE " + 
                "table_schema = '%s' AND table_name = '%s'", 
                table.getDatabase().getName(), 
                table.getName());
        final List<Map<String, String>> sqlResult = executeQuery(sql);
        
        for(final Map<String, String> result : sqlResult){
            
            Column col = new Column();
            col.setTable(table);
            col.setName(result.get("COLUMN_NAME"));
            col.setDataType(result.get("DATA_TYPE"));
            col.setPosition(Integer.parseInt(result.get("ORDINAL_POSITION")) - 1); // make value start from 0
            
            table.addColumn(col);
            
        }
        
    }
    
    public void crawlIndexes(Table table) throws SQLException{
        
        final String sql = String.format(
                "SELECT * FROM INFORMATION_SCHEMA.STATISTICS WHERE " + 
                "table_schema = '%s' AND " + 
                "table_name = '%s' ",
                table.getDatabase().getName(),
                table.getName());
        
        final List<Map<String, String>> sqlResult = executeQuery(sql);
        Index idx = null;
        for(final Map<String, String> result : sqlResult){
         
            if(result.get("SEQ_IN_INDEX").equals("1")){
                idx = new Index();
                idx.setTable(table);
                idx.setName(result.get("INDEX_NAME"));
                idx.setUnique(result.get("NON_UNIQUE").equals("0"));
                
                table.addIndex(idx);
                
            }
            
            final Column col = table.getColumn(result.get("COLUMN_NAME"));
            idx.addColumn(col);
            
        }
        
    }
    
}
