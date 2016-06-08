package com.kakao.adt.test.tool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DataIntegrityTestTool extends Thread {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataIntegrityTestTool.class);
    
    private final MysqlDataSource srcDs;
    private final MysqlDataSource[] destDs;
    private final int destCount;
    
    public DataIntegrityTestTool() throws Exception{
        this(System.getProperties());
    }
    
    public DataIntegrityTestTool(Properties prop) throws Exception{
        
        this.setName("data-integrity-test-thread");
        
        srcDs = new MysqlDataSource();
        srcDs.setURL(prop.getProperty(DMLQueryTool.PROP_MSR_SRC_URL));
        srcDs.setUser(prop.getProperty(DMLQueryTool.PROP_MSR_SRC_USERNAME));
        srcDs.setPassword(prop.getProperty(DMLQueryTool.PROP_MSR_SRC_PASSWROD));
        
        destCount = Integer.parseInt(prop.getProperty(DMLQueryTool.PROP_MSR_DEST_COUNT));
        destDs = new MysqlDataSource[destCount];
        for(int i=0; i<destCount; i++){
            destDs[i] = new MysqlDataSource();
            destDs[i].setURL(prop.getProperty(String.format(DMLQueryTool.PROP_MSR_DEST_URL, i)));
            destDs[i].setUser(prop.getProperty(String.format(DMLQueryTool.PROP_MSR_DEST_USERNAME, i)));
            destDs[i].setPassword(prop.getProperty(String.format(DMLQueryTool.PROP_MSR_DEST_PASSWORD, i)));
        }
    }
    
    public void run() {
            
        while(true){
            for(int i=0; i<60; i++){
                
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                System.out.println("running (" + i + ")");
            }

            try {
                runTest();
            } catch (Exception e) {
                System.out.println("TEST MONITOR THREAD ERROR");
                e.printStackTrace();
            }
        }
            
        
    }
    
    public boolean runTest() throws Exception{
        
        final List<String> srcData = new ArrayList<>();
        List<String> destData = new ArrayList<>();
        
        System.out.println("[START] data integrity test");
        Connection srcConn = null;
        try{
            srcConn = srcDs.getConnection();
            srcConn.setAutoCommit(false);
            Statement srcStmt = srcConn.createStatement();
            
            System.out.println("== gather src data ==");
            // get master's whole data 
            for(String tableName : DMLQueryTool.TABLE_NAMES){
                srcData.addAll(getHashedValues(srcStmt, tableName, true));
            }
            
            System.out.println("== gather dst data ==");
            // get slaves' whole data
            while(true){
                
                final List<String> tempDestData = new ArrayList<>();
                
                for(int i=0; i<destCount; i++){
                    for(int j=0; j<DMLQueryTool.TABLE_NAMES.length; j++){
                        final String tableName = DMLQueryTool.TABLE_NAMES[j];
                        final List<String> data = getDestData(i, tableName);
                        tempDestData.addAll(data);
                    }
                }
                
                if(destData.equals(tempDestData)){
                    break;
                }
                else{
                    destData = tempDestData;
                    System.out.println("[WAIT] adt is still working. wait 3sec");
                    Thread.sleep(3000);
                }
                
            }
        }finally{
            if(srcConn != null){
                srcConn.rollback();
                srcConn.close();
            }
        }
        
        srcData.sort(null);
        destData.sort(null);
        System.out.println("src_data_cnt=" + srcData.size());
        System.out.println("dst_data_cnt=" + destData.size());
        
        boolean valid = true;
        if(srcData.size() != destData.size()){
            System.out.println("src_size=" + srcData.size() + ", dest_size=" + destData.size());
            valid = false;
        }
        else{
            for(int i=0; i<srcData.size(); i++){
                if(srcData.get(i).equals(destData.get(i))){
                    continue;
                }
                System.out.println("i=" + i + ", src=" + srcData.get(i) + ", dest=" + destData.get(i));
                valid = false;
                break;
            }
        }
        
        if(valid){
            System.out.println("[SUCCESS] finished checking data integrity");
            return true;
        }
        else{
            System.out.println("[FAIL] Data Integrity is INVALID");
            System.out.println("source data list");
            for(String row : srcData){
                System.out.println("[SRC] " + row);
            }
            
            System.out.println("destination data list");
            for(String row : destData){
                System.out.println("[DST] " + row);
            }
            return false;
        }
            
        
    }
    
    private List<String> getDestData(int shardIndex, String tableName) throws Exception{
        Connection conn = null;
        try{
            conn = destDs[shardIndex].getConnection();
            conn.setAutoCommit(true);
            
            Statement stmt = conn.createStatement();
            List<String> result = getHashedValues(stmt, tableName, false);
            stmt.close();
            
            return result;
            
        }finally{
            if(conn != null){
                conn.close();
            }
        }
    }
    
    private List<String> getHashedValues(Statement stmt, String tableName, boolean lock) throws Exception{
        
        System.out.println("start gathering data (table=" + tableName + ")");
        
        final String selectHashValueSql = 
                "SELECT CONCAT_WS('_', '[" + tableName + "]', no, seq, uk, v, c) " + 
                "FROM " + tableName + (lock ? " FOR UPDATE" : "");
        final List<String> result = new ArrayList<>();
        
        final ResultSet rs = stmt.executeQuery(selectHashValueSql);
        while(rs.next()){
            result.add(rs.getString(1));
        }
        rs.close();
        
        System.out.println("finished gathering data (row_count=" + result.size() + ")");
        
        return result;
        
    }
    
    public static void main(String[] args) throws Exception{
        DataIntegrityTestTool test = new DataIntegrityTestTool();
        try{
            test.runTest();
        }catch(Exception e){
            e.printStackTrace();
        }
        System.out.println("FINISH DATA INTEGRITY CHECKER");
    }
    
}
