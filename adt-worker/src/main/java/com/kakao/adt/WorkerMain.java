package com.kakao.adt;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kakao.adt.handler.MysqlBinlogProcessorHandler;
import com.kakao.adt.handler.MysqlCrawlProcessorHandler;
import com.kakao.adt.handler.ProcessorHandler;
import com.kakao.adt.mysql.binlog.MysqlBinlogProcessor;
import com.kakao.adt.mysql.binlog.MysqlBinlogProcessorConfig;
import com.kakao.adt.mysql.crawler.MysqlCrawlProcessor;
import com.kakao.adt.mysql.crawler.MysqlCrawlProcessorConfig;

public class WorkerMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerMain.class);
    
    public static final String PROP_KEY_WORKER_TYPE = "com.kakao.adt.worker.type";
    public static final String PROP_KEY_WORKER_CONFIG_FILE_PATH = "com.kakao.adt.worker.configFilePath";
    
    private volatile static IProcessor<?> processorInstance;
    
    //==========================================================
    //
    //      Static methods
    //
    //==========================================================

    public static void main(String[] args) {
        try{
            final WorkerType workerType = WorkerType.valueOf(getProperty(PROP_KEY_WORKER_TYPE, false));
            final String workerConfigFilePathStr = getProperty(PROP_KEY_WORKER_CONFIG_FILE_PATH, false);
            final Path workerConfigFilePath = Paths.get(getProperty("user.dir", false), workerConfigFilePathStr);
            final byte[] workerConfigFileByteArr = Files.readAllBytes(workerConfigFilePath);
            final String workerConfigJsonStr = new String(workerConfigFileByteArr, "UTF-8");
            LOGGER.info("adt config\n" + workerConfigJsonStr);
            final Object configObj = workerType.parseConfigJson(workerConfigJsonStr);
            workerType.execute(configObj);
            
        }catch(Exception e){
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
            System.exit(1);
        }
        
        LOGGER.info("SHUTDOWN NOW!");
        
    }
    
    public static enum WorkerType{
        Test(ProcessorHandler.class, Map.class) {
            @Override
            public void execute(Object _config) throws Exception {
                LOGGER.info("THIS WORKER TYPE IS FOR TEST.");
                LOGGER.info("NOW, QUITTING...");
            }
        },
        MysqlBinlogReceiver(MysqlBinlogProcessorHandler.class, MysqlBinlogProcessorConfig.class) {
            @Override
            public void execute(Object _config) throws Exception {
                MysqlBinlogProcessorConfig config = (MysqlBinlogProcessorConfig) _config;
                MysqlBinlogProcessor processor = new MysqlBinlogProcessor(config);
                processorInstance = processor;
                processor.start();
                processor.waitStop();
                processorInstance = null;
                
            }
        },
        MysqlTableCrawler(MysqlCrawlProcessorHandler.class, MysqlCrawlProcessorConfig.class) {
            @Override
            public void execute(Object _config) throws Exception {
                MysqlCrawlProcessorConfig config = (MysqlCrawlProcessorConfig) _config;
                MysqlCrawlProcessor processor = new MysqlCrawlProcessor(config);
                processorInstance = processor;
                processor.start();
                processor.waitStop();
                processorInstance = null;
            }
        },
        /*
         * OracleTableCrawl,
         * PgSqlTableCrawl,
         * HBaseCrawl,
         * etc...
         */
        ;
        
        private final Class<?> workerHandlerClass;
        private final Class<?> configClass;
        private final ObjectMapper mapper = new ObjectMapper();
        
        private WorkerType(Class<?> workerHandlerClass, Class<?> configClass){
            this.workerHandlerClass = workerHandlerClass;
            this.configClass = configClass;
        }
        
        public Object parseConfigJson(String json) throws JsonParseException, JsonMappingException, IOException{
            return mapper.readValue(json, configClass);
        }
        
        public void validateWorkerHandler(Object obj){
            if(!workerHandlerClass.isInstance(obj)){
                System.exit(1);
            }
        }
        
        public abstract void execute(Object _config) throws Exception;
        
    }
    
    /**
     * use ONLY for TEST
     * @return
     */
    public static IProcessor<?> getProcessorInstance(){
        return processorInstance;
    }

    public static String getProperty(String key, boolean isOptional){
        String value = System.getProperty(key);
        if(!isOptional && value == null){
            LOGGER.error("property [" + key +  "] is required.");
            System.exit(1);
        }
        return value;
    }
    
    public static Integer getPropertyAsInteger(String key, boolean isOptional){
        String valueStr = getProperty(key, isOptional);
        try{
            return Integer.parseInt(valueStr);
        }catch(Exception e){
            LOGGER.error(e.getMessage(), e);
            System.exit(1);
            return null;
        }
    }
    
    

    
}
