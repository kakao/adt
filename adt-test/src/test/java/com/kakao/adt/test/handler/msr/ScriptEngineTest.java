package com.kakao.adt.test.handler.msr;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import com.kakao.adt.mysql.binlog.MysqlBinlogData;

import org.slf4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Ignore
public class ScriptEngineTest {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(ScriptEngineTest.class);
    
    private static String scriptString = "";
    private static ScriptEngine scriptEngine = null;
    private static Invocable invocable = null;
    
    public static String readResourceAsString(String resPath) throws IOException{
        final InputStream is = ScriptEngineTest.class.getClassLoader().getResourceAsStream(resPath);
        final InputStreamReader reader = new InputStreamReader(is);
        final BufferedReader bufferedReader = new BufferedReader(reader);
        
        String result = "";
        while(true){
            String aLine = bufferedReader.readLine();
            if(aLine == null){
                break;
            }
            result += aLine + "\n";
        }
        
        bufferedReader.close();
        reader.close();
        is.close();
        
        return result;
    }
    
    @BeforeClass
    public static void beforeClass() throws Exception{
        scriptString = readResourceAsString("test_script.js");
        
        final ScriptEngineManager engineManager = new ScriptEngineManager();
        scriptEngine = engineManager.getEngineByName("nashorn");
        invocable = (Invocable) scriptEngine;
        
        scriptEngine.eval(scriptString);
        
    }
    
    @Test
    public void test_print() throws Exception{
        
        final Map<String, Object> testArg1 = new HashMap<>();
        testArg1.put("1", 111);
        testArg1.put("2", 222222222222L);
        testArg1.put("3", "3");
        
        final MysqlBinlogData testArg2 = new MysqlBinlogData(null, null, null, null);
        
        invocable.invokeFunction("func_print", testArg1);
        invocable.invokeFunction("func_print", testArg2);
        
    }
    
    @Test
    public void test_access_list() throws Exception {
        
        final List<Object> arg = new ArrayList<>();
        arg.add("abc");
        arg.add(12345);
        
        invocable.invokeFunction("func_iterate_list", arg);
        Assert.assertEquals(invocable.invokeFunction("func_get_from_list", arg, 0), "abc");
        Assert.assertEquals(invocable.invokeFunction("func_get_from_list", arg, 1), 12345);
        
    }
    
    @Test
    public void test_nested_structure() throws Exception{
        Assert.assertEquals(123, 
                invocable.invokeMethod(scriptEngine.get("testVar"), "testFunc"));
        Assert.assertEquals("abcabc", 
                invocable.invokeMethod(scriptEngine.eval("testVar.testVar2"), "testFunc2", "abc"));
        
    }
    
    
}
