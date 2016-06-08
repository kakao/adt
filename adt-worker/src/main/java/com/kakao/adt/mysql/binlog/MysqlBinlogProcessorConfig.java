package com.kakao.adt.mysql.binlog;

import java.lang.reflect.Field;
import java.util.List;

public class MysqlBinlogProcessorConfig {

    // ===============================
    //
    // Binlog receiver Configs
    //
    // ===============================

    public String host;

    public Integer port;

    public String user;

    public String password;

    public List<String> databaseList;

    public Integer mysqlSlaveServerId;

    public String binlogFileName;

    public Integer binlogFilePosition;

    /**
     * dependency가 있는 값들(PK, UK)을 순차적으로 처리하기 위해 내부적으로 여러 queue가 존재함. 어느 queue에
     * 넣을지는 (PK, UK 등의 값들의 hash값) % taskQueueCount 의 공식으로 결정함. 이 값이 작으면
     * dependency가 없는 event들이 같은 queue에 들어갈 확률이 높음. 반면, 이 값이 크면 dependency가 없는
     * event들이 병렬로 실행되지만 그만큼 메모리 할당량이 많아짐. 테스트할 때 1024개씩 만들어도 큰 문제는 없었음
     */

    public Integer taskQueueCount;

    /**
     * binlog event를 쌓을 버퍼 binlog 읽는 속도가 처리하는 속도보다 빠르다면 out of memory 조심해야함
     */

    public Integer eventBufferSize;

    public Integer workerThreadCorePoolSize;
    
    public String handlerClassName;

    public Integer eventBufferInputFailSleepTime = 100;
    
    public Integer taskQueueMaxCount = 100;
    
    public Integer taskQueueInputFailSleepTime = 100;
    
    public Long binlogReceiverStopTimeout = 1000L;
    
    public Long threadPoolTerminationTimeout = 1000L;
    
    
    public void checkValidation() throws Exception{
        Field[] fields = this.getClass().getFields();
        
        for(Field field : fields){
            if(null == field.get(this)){
                throw new Exception("field [" + field.getName() + "] is null.");
            }
        }
        
    }
    
}
