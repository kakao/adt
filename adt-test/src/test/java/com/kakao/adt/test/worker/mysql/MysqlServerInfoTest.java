package com.kakao.adt.test.worker.mysql;

import java.io.*;
import java.sql.*;
import java.util.*;

import org.junit.*;
import org.slf4j.*;

import com.kakao.adt.mysql.metadata.*;

import static com.kakao.adt.test.TestUtil.*;
import static org.junit.Assert.*;


public class MysqlServerInfoTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlServerInfoTest.class);
    
    private static Connection conn;
    
    @BeforeClass
    public static void beforeClass() throws Exception{
        initializeTestEnv();
        conn = testDataSource.getConnection();
    }
    
    @AfterClass
    public static void afterClass() throws Exception {
        if(conn != null){
            conn.close();
        }
    }
    
    @Test
    public void test_createMysqlServerInfo() throws Exception{
        
        MysqlServerInfo schema = new MysqlServerInfo(
                DB_HOST, 
                DB_PORT, 
                DB_USER,
                DB_PW,
                Arrays.asList(new String[]{DB_TEST_SCHEMA}));
        
        final Database[] dbList = schema.getDatabases().toArray(new Database[0]);
        
        assertTrue(dbList.length == 1);
        assertTrue(dbList[0].getName().equals(DB_TEST_SCHEMA));
        
        final Database db = dbList[0];
        
        final Table[] tableList = db.getTables().toArray(new Table[0]);
        
        assertTrue(tableList.length == 3);
        
        for(final Table table: tableList){
            
            for(int i=0; i<table.getColumns().size(); i++){
                assertTrue(i == table.getColumns().get(i).getPosition());
            }
            
            if(table.getName().equals("adt_test_1")){
                assertTrue(table.getColumns().size() == 7);
                assertTrue(table.getIndexes().size() == 4);
                
                boolean checkedPK = false;
                boolean checkedUK = false;
                
                for(final Index idx : table.getIndexes()){
                    if(idx.getName().equalsIgnoreCase("primary")){
                        checkedPK = true;

                        assertTrue(idx.isUnique());
                        assertTrue(idx.getColumns().size() == 2);
                        assertTrue(idx.getColumn(0).getName().equals("no"));
                        assertTrue(idx.getColumn(1).getName().equals("seq"));
                        assertTrue(idx.getColumn(0) == table.getColumn("no"));
                        assertTrue(idx.getColumn(1) == table.getColumn("seq"));
                    }
                    else if(idx.getName().equalsIgnoreCase("ux_uk_no")){
                        checkedUK = true;
                        assertTrue(idx.isUnique());
                        assertTrue(idx.getColumns().size() == 2);
                        assertTrue(idx.getColumn(0).getName().equals("uk"));
                        assertTrue(idx.getColumn(1).getName().equals("no"));
                        assertTrue(idx.getColumn(0) == table.getColumn("uk"));
                        assertTrue(idx.getColumn(1) == table.getColumn("no"));
                    }
                    else{
                        assertTrue(!idx.isUnique());
                    }
                }
                
                assertTrue(checkedPK);
                assertTrue(checkedUK);
            }
            else if(table.getName().equals("adt_test_2")){
                
            }
            else if(table.getName().equals("adt_test_3")){
                assertTrue(table.getColumns().size() == 2);
                assertTrue(table.getIndexes().size() == 1);
            }
            else{
                fail("Unknown table. name=" + table.getName());
            }
        }
        
    }
    
    
    
}
