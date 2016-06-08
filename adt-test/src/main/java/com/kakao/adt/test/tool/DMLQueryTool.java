package com.kakao.adt.test.tool;

import java.io.*;
import java.sql.*;
import javax.sql.*;
import java.util.*;
import java.util.concurrent.*;

import org.apache.commons.dbcp2.BasicDataSource;
import org.slf4j.*;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSourceFactory;


public class DMLQueryTool implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(DMLQueryTool.class);

    public static final String PROP_MSR_SRC_URL = "msr.src.url";
    public static final String PROP_MSR_SRC_USERNAME = "msr.src.username";
    public static final String PROP_MSR_SRC_PASSWROD = "msr.src.password";

    public static final String PROP_MSR_DEST_COUNT = "msr.dest.count";
    public static final String PROP_MSR_DEST_URL = "msr.dest.%d.url";
    public static final String PROP_MSR_DEST_USERNAME = "msr.dest.%d.username";
    public static final String PROP_MSR_DEST_PASSWORD = "msr.dest.%d.password";

    public static final String PROP_MSR_TOOL_DQT_THREAD_COUNT = "msr.tool.dmlQueryTool.threadCount";
    public static final String PROP_MSR_TOOL_DQT_DBCP2_MAX_TOTAL = "msr.tool.dmlQueryTool.dbcp2.maxTotal";
    public static final String PROP_MSR_TOOL_DQT_DBCP2_MAX_IDLE = "msr.tool.dmlQueryTool.dbcp2.maxIdle";
    public static final String PROP_MSR_TOOL_DQT_DBCP2_MIN_IDLE = "msr.tool.dmlQueryTool.dbcp2.minIdle";

    public static final String PROP_MSR_TOOL_DQT_TABLE_COUNT =  "msr.tool.dmlQueryTool.tableCount";
    public static final String PROP_MSR_TOOL_DQT_MAX_NO =       "msr.tool.dmlQueryTool.table.%d.maxNoValue";
    public static final String PROP_MSR_TOOL_DQT_MAX_SEQ =      "msr.tool.dmlQueryTool.table.%d.maxSeqValue";
    public static final String PROP_MSR_TOOL_DQT_MAX_UK =       "msr.tool.dmlQueryTool.table.%d.maxUkValue";
    public static final String PROP_MSR_TOOL_DQT_QUERY_RATIO =  "msr.tool.dmlQueryTool.table.%d.queryRatio.%s";

    public static final String[] TABLE_NAMES = new String[]{"adt_test_1", "adt_test_2"};

    public static DMLQueryTool instance = null;

    public static void main(String[] args) throws Exception{
        try{
            instance = new DMLQueryTool();
            Thread.sleep(Long.MAX_VALUE);
        }catch(Exception e){
            LOGGER.error(e.getMessage(), e);
        }

    }

    public static String getResourceAsString(String path) throws IOException{
        final InputStream inStream = DMLQueryTool.class.getClassLoader().getResourceAsStream(path);
        final InputStreamReader inReader = new InputStreamReader(inStream);
        final BufferedReader bufReader = new BufferedReader(inReader);

        final StringBuilder result = new StringBuilder();
        while(true){
            final String line = bufReader.readLine();
            if(line == null){
                bufReader.close();
                inReader.close();
                inStream.close();
                return result.toString();
            }
            result.append(line).append("\n");
        }

    }

    public static Properties getProperties(String path) throws IOException {
        Properties prop = new Properties();
        prop.load(DMLQueryTool.class.getClassLoader().getResourceAsStream(path));
        return prop;
    }

    //====================================================
    //
    //
    //
    //====================================================

    private final Properties prop;
    private final BasicDataSource dataSource = new BasicDataSource();
    private final ThreadPoolExecutor threadPool;

    private final int tableCount;
    private final int[] maxNoValue;
    private final int[] maxSeqValue;
    private final int[] maxUkValue;

    private final int totalRatio;
    private final int[] tableRatio;
    private final int[][] queryRatio;

    private final String[][] preparedQuery;

    public DMLQueryTool() throws IOException, SQLException{

        this(System.getProperties());

    }

    public DMLQueryTool(String propPath) throws IOException, SQLException{
        this(getProperties(propPath));
    }

    public DMLQueryTool(Properties prop) throws IOException, SQLException{

        this.prop = prop;

        dataSource.setUrl(prop.getProperty(PROP_MSR_SRC_URL));
        dataSource.setUsername(prop.getProperty(PROP_MSR_SRC_USERNAME));
        dataSource.setPassword(prop.getProperty(PROP_MSR_SRC_PASSWROD));
        dataSource.setMaxTotal(Integer.parseInt(prop.getProperty(PROP_MSR_TOOL_DQT_DBCP2_MAX_TOTAL)));
        dataSource.setMaxIdle(Integer.parseInt(prop.getProperty(PROP_MSR_TOOL_DQT_DBCP2_MAX_IDLE)));
        dataSource.setMinIdle(Integer.parseInt(prop.getProperty(PROP_MSR_TOOL_DQT_DBCP2_MIN_IDLE)));

        final int threadCount = Integer.parseInt(prop.getProperty(PROP_MSR_TOOL_DQT_THREAD_COUNT));
        threadPool = new ThreadPoolExecutor(threadCount, threadCount, 10, TimeUnit.MINUTES, new LinkedBlockingQueue<>());


        tableCount = Integer.parseInt(prop.getProperty(PROP_MSR_TOOL_DQT_TABLE_COUNT));
        maxNoValue = new int[tableCount];
        maxSeqValue = new int[tableCount];
        maxUkValue = new int[tableCount];
        tableRatio = new int[tableCount];
        queryRatio = new int[tableCount][DMLQueryType.values().length];

        int queryRatioSum = 0;
        for(int i=0; i<tableCount; i++){
            maxNoValue[i] = Integer.parseInt(prop.getProperty(String.format(PROP_MSR_TOOL_DQT_MAX_NO, i)));
            maxSeqValue[i] = Integer.parseInt(prop.getProperty(String.format(PROP_MSR_TOOL_DQT_MAX_SEQ, i)));
            maxUkValue[i] = Integer.parseInt(prop.getProperty(String.format(PROP_MSR_TOOL_DQT_MAX_UK, i)));
            tableRatio[i] = 0;
            for(DMLQueryType queryType : DMLQueryType.values()){
                final String propKey = String.format(PROP_MSR_TOOL_DQT_QUERY_RATIO, i, queryType.getPropKey());
                queryRatio[i][queryType.getQueryIndex()] = Integer.parseInt(prop.getProperty(propKey));
                tableRatio[i] += queryRatio[i][queryType.getQueryIndex()];
                queryRatioSum += queryRatio[i][queryType.getQueryIndex()];
            }
        }
        this.totalRatio = queryRatioSum;

        this.preparedQuery = new String[tableCount][DMLQueryType.values().length];
        for(int i=0; i<tableCount; i++){
            final String tableName = TABLE_NAMES[i];
            for(DMLQueryType queryType : DMLQueryType.values()){
                this.preparedQuery[i][queryType.getQueryIndex()] = generatePreparedQuery(queryType, tableName);
            }
        }

        if(totalRatio <= 0 || totalRatio <= 0){
            throw new IllegalArgumentException("ratio total value can't be 0 or less.");
        }

        for(int i=0; i<threadCount; i++){
            threadPool.execute(this);
        }


    }

    @Override
    public void run() {
        final Random random;
        synchronized(this){
            random = new Random(Long.parseLong(System.currentTimeMillis() + "" + Thread.currentThread().getId()));
        }
        while(true){
            Connection conn = null;
            try{
                conn = this.dataSource.getConnection();

                final int tableNum = getRatioIndex((int)(random.nextDouble() * totalRatio), tableRatio);
                final int queryNum = getRatioIndex((int)(random.nextDouble() * tableRatio[tableNum]), queryRatio[tableNum]);
                final DMLQueryType queryType = DMLQueryType.getQueryTypeByIndex(queryNum);

                final String sql = preparedQuery[tableNum][queryNum];
                final PreparedStatement pstmt;

                if( queryType == DMLQueryType.Insert ||
                    queryType == DMLQueryType.Replace ||
                    queryType == DMLQueryType.InsertIgnore ||
                    queryType == DMLQueryType.InsertOnDupKey){

                    pstmt = conn.prepareStatement(sql);
                    pstmt.setInt(1, (int)(random.nextDouble() * maxNoValue[tableNum]));
                    pstmt.setInt(2, (int)(random.nextDouble() * maxSeqValue[tableNum]));
                    pstmt.setInt(3, (int)(random.nextDouble() * maxUkValue[tableNum]));
                    pstmt.setString(4, "가나다ABC_" + random.nextDouble());
                }
                else if(queryType == DMLQueryType.Update){
                    pstmt = conn.prepareStatement(sql);
                    pstmt.setInt(1, (int)(random.nextDouble() * maxNoValue[tableNum]));
                    pstmt.setInt(2, (int)(random.nextDouble() * maxSeqValue[tableNum]));
                    pstmt.setInt(3, (int)(random.nextDouble() * maxUkValue[tableNum]));
                    pstmt.setString(4, "가나다ABC_" + random.nextDouble());
                    pstmt.setInt(5, (int)(random.nextDouble() * maxNoValue[tableNum]));
                    pstmt.setInt(6, (int)(random.nextDouble() * maxSeqValue[tableNum]));

                }
                else if(queryType == DMLQueryType.Delete){

                    pstmt = conn.prepareStatement(sql);
                    pstmt.setInt(1, (int)(random.nextDouble() * maxNoValue[tableNum]));
                    pstmt.setInt(2, (int)(random.nextDouble() * maxSeqValue[tableNum]));

                }
                else{
                    throw new IllegalStateException("UNKNOWN QUERY TYPE: " + queryType);
                }

                // TODO int affectedRowCount =
                pstmt.executeUpdate();

            }catch(Exception e){
                if(e.getMessage().indexOf("Table 'adt.adt_test' doesn't exist") == 0){
                    // DO NOTHING
                }
                else if(e.getMessage().indexOf("Table 'adt.adt_test_2' doesn't exist") == 0){
                    // DO NOTHING
                }
                else if(e.getMessage().indexOf("Duplicate entry ") == 0){
                    // DO NOTHING
                }
                else if(e.getMessage().indexOf("Communications link failure") == 0){
                    // DO NOTHING
                }
                else if(e.getMessage().indexOf("Lock wait timeout exceeded") == 0){
                    // DO NOTHING
                }
                else if(e.getMessage().indexOf("Deadlock found when trying to get lock") == 0){
                    // DO NOTHING
                }
                else{
                    LOGGER.error(e.getMessage(), e);
                }
            } finally{
                try {
                    if(conn != null){
                        conn.close();
                    }
                } catch (SQLException e) {
                    LOGGER.error(e.getMessage(), e);
                }
            }
        }

    }

    public String generatePreparedQuery(DMLQueryType queryType, String tableName){
        final StringBuilder sqlBuilder = new StringBuilder(4*1024);
        if( queryType == DMLQueryType.Insert ||
            queryType == DMLQueryType.Replace ||
            queryType == DMLQueryType.InsertIgnore ||
            queryType == DMLQueryType.InsertOnDupKey){ // INSERT

            sqlBuilder.append(
                    queryType == DMLQueryType.Replace ? "REPLACE INTO " :
                    queryType == DMLQueryType.InsertIgnore ? "INSERT IGNORE INTO " :
                    "INSERT INTO "); // INSERT, INSERT ON DUPLICATE KEY
            sqlBuilder.append(tableName).append(" SET ")
                .append("no = ?, ")
                .append("seq = ?, ")
                .append("uk = ?, ")
                .append("v = ?, ")
                .append("c = 0, ")
                .append("regtime = CURRENT_TIMESTAMP ")
                .append(queryType == DMLQueryType.InsertOnDupKey ? " ON DUPLICATE KEY UPDATE c = c + 1 " : "");
        }else if(queryType == DMLQueryType.Update){ // UPDATE
            sqlBuilder.append("UPDATE ").append(tableName).append(" SET ")
                .append("no = ?, ")
                .append("seq = ?, ")
                .append("uk = ?, ")
                .append("v = ?, ")
                .append("c = c + 1 ")
                .append(" WHERE no = ? AND seq = ? ");

        }
        else if(queryType == DMLQueryType.Delete){ // DELETE
            sqlBuilder.append("DELETE FROM ").append(tableName).append(" WHERE no = ? AND seq = ?");

        }
        else{
            throw new IllegalStateException("UNKNOWN QUERY TYPE: " + queryType);
        }

        final String result = sqlBuilder.toString();
        return result;
    }

    public int getRatioIndex(int ratioRandom, int ...ratioArray){

        int accRatio = 0;
        for(int i=0; i<ratioArray.length; i++){
            accRatio += ratioArray[i];
            if(ratioRandom < accRatio){
                return i;
            }
        }

        throw new IllegalStateException("PANIC!");
    }



}
