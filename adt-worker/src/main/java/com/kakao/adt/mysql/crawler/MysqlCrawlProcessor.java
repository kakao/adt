package com.kakao.adt.mysql.crawler;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kakao.adt.IProcessor;
import com.kakao.adt.handler.MysqlBinlogProcessorHandler;
import com.kakao.adt.handler.MysqlCrawlProcessorHandler;
import com.kakao.adt.handler.ProcessorHandler;
import com.kakao.adt.mysql.metadata.*;
import com.kakao.adt.parallel.AbstractIOIExecutor;

public class MysqlCrawlProcessor extends AbstractIOIExecutor<Table, Boolean> implements IProcessor<MysqlCrawlData>{

    private static final Logger LOGGER = LoggerFactory.getLogger(MysqlCrawlProcessor.class);
    
    private final MysqlCrawlProcessorConfig config;
    private final MysqlServerInfo serverInfo;
    private AtomicLong completeTableCount = new AtomicLong(0);
    private final BasicDataSource dataSource;
    private final MysqlCrawlProcessorHandler handler;
    
    private final CountDownLatch waitStopLatch = new CountDownLatch(1);
    private final AtomicReference<Exception> execException = new AtomicReference<Exception>(null);
    
    public MysqlCrawlProcessor(MysqlCrawlProcessorConfig config) throws Exception{
        
        super(Math.max(config.tableList.size() + 1, config.concurrentCrawlCount), 
                new ThreadPoolExecutor(
                        config.concurrentCrawlCount, 
                        config.concurrentCrawlCount, 
                        10, 
                        TimeUnit.MINUTES, 
                        new LinkedBlockingQueue<>()));
        
        config.checkValidation();
        this.config = config;
        
        Set<String> dbNameSet = new HashSet<>();
        for(String tableName : config.tableList){
            dbNameSet.add(tableName.split("\\.")[0]);
        }
        this.serverInfo = new MysqlServerInfo(config.host, config.port, config.user, config.password, new ArrayList<>(dbNameSet));
        
        // data source
        final String url = "jdbc:mysql://" + config.host + ":" + config.port + "/?" + config.jdbcConnParam;
        this.dataSource = BasicDataSourceFactory.createDataSource(config.dbcp2Properties);
        this.dataSource.setUrl(url);
        this.dataSource.setUsername(config.user);
        this.dataSource.setPassword(config.password);
        
        // init handler
        @SuppressWarnings("unchecked")
        final Class<? extends MysqlCrawlProcessorHandler> handlerClass = 
                (Class<? extends MysqlCrawlProcessorHandler>) Class.forName(config.handlerClassName);
        handler = handlerClass.newInstance();
        if(!(handler instanceof MysqlCrawlProcessorHandler)){
            throw new IllegalArgumentException("Class [" + config.handlerClassName + "] is invalid.");
        }
        
    }
    
    public void start() throws SQLException{
        
        getHandler().beforeStart();
        
        for(Database database : this.serverInfo.getDatabases()){
            for(Table table : database.getTables()){
                
                final String tableFullName = database.getName() + "." + table.getName();
                if(!config.tableList.contains(tableFullName)){
                    LOGGER.info("skip table: " + tableFullName);
                    continue;
                }else{
                    LOGGER.info("crawl table: " + tableFullName);
                }
                
                while(!input(table)){
                    LOGGER.warn("impossible situation is happened.");
                    try{
                        Thread.sleep(1000);
                    }catch(Exception e){
                    }
                }
            }
        }
    }
    
    public void stop(){
        getHandler().afterStop();
        
        waitStopLatch.countDown();
    }
    
    @Override
    public void waitStop() throws Exception {
        waitStopLatch.await();
        
        if(execException.get() != null){
            throw new RuntimeException(execException.get());
        }
    }
    
    @Override
    public MysqlCrawlProcessorHandler getHandler() {
        return this.handler;
    }
    
    public MysqlServerInfo getServerInfo(){
        return serverInfo;
    }
    
    public long getCompleteTableCount(){
        return completeTableCount.get();
    }
    
    public MysqlCrawlProcessorConfig getConfig(){
        return this.config;
    }
    
    public DataSource getDataSource(){
        return this.dataSource;
    }
    
    public boolean isComplete(){
        return config.tableList.size() == completeTableCount.get();
    }
    
    //====================================
    //
    //  AbstractIOIExecutor
    //
    //====================================
    
    @Override
    public int getMaxWorkerCount() {
        return this.threadPool.getMaximumPoolSize();
    };
    
    @Override
    public void onException(Exception e){
        LOGGER.error(e.getMessage(), e);
        execException.compareAndSet(null, e);
        stop();
    }
    
    @Override
    public Boolean process(Table table) {
        try{

            MysqlTableCrawlTask task = new MysqlTableCrawlTask(this, table);
            task.crawl();
            return true;
        }catch(Exception e){
            onException(e);
            return false;
        }
    };
    
    @Override
    public void completeHandler(Boolean output) {
        this.outputComplete();
        
        long curCompleteCount = completeTableCount.incrementAndGet();
        if(curCompleteCount == config.tableList.size()){
            stop();
        }
        
    };
    
    
    //====================================
    //
    //  IProcessor
    //
    //====================================
    
    @Override
    public void processData(MysqlCrawlData crawlData){
        try {
            getHandler().processData(crawlData);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            onException(e);
        }
    }
    
}
