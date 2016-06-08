package com.kakao.adt.mysql.crawler;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;

import com.kakao.adt.handler.MysqlCrawlProcessorHandler;

public class MysqlCrawlProcessorConfig {

    public String host;

    public Integer port;

    public String user;

    public String password;
    
    public String jdbcConnParam;
    
    /**
     * @see org.apache.commons.dbcp2.BasicDataSourceFactory
     * @see org.apache.commons.dbcp2.BasicDataSource 
     */
    public Properties dbcp2Properties;
    
    /**
     * [db name].[table name]
     */
    public List<String> tableList; 
    
    public Integer selectLimitCount;
    
    public MysqlSelectLockMode selectLockMode;
    
    /**
     * 동시에 몇 개의 테이블을 크롤링할 것인가에 대한 값
     * 1 이상의 값이어야 함  
     */
    public Integer concurrentCrawlCount;
    
    /**
     * 테이블 별 preload할 스레드 개수
     * 1 이상의 값이어야 함
     */
    public Integer workerThreadCountPerTable;
    
    public String handlerClassName;
    
    public void checkValidation() throws Exception{
        
        Field[] fields = this.getClass().getFields();
        
        for(Field field : fields){
            if(null == field.get(this)){
                throw new Exception("field [" + field.getName() + "] is null.");
            }
        }
        
    }
    
    
}
