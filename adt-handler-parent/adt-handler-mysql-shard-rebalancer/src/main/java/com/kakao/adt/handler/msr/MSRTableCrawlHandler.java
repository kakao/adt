package com.kakao.adt.handler.msr;

import java.util.List;

import javax.script.ScriptException;

import com.kakao.adt.handler.MysqlCrawlProcessorHandler;
import com.kakao.adt.mysql.crawler.MysqlCrawlData;
import com.kakao.adt.mysql.metadata.Table;

public class MSRTableCrawlHandler extends AbstractMysqlHandler implements MysqlCrawlProcessorHandler {

    public MSRTableCrawlHandler() throws Exception {
        super();
    }

    @Override
    public void beforeStart() {
        // TODO Auto-generated method stub

    }

    @Override
    public void afterStop() {
        // TODO Auto-generated method stub

    }

    @Override
    public void onException(Exception e) {
        // TODO Auto-generated method stub

    }

    @Override
    public void processData(MysqlCrawlData data) throws Exception {
        
        final List<List<Object>> rowList = data.getAllRows();
        final Table table = data.getTable(); 
        
        for(int i=0; i<rowList.size(); i++){
            final List<Object> row = rowList.get(i);
            insertIgnore(table, row);
        }

    }

}
