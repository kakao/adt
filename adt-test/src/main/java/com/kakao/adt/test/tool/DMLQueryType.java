package com.kakao.adt.test.tool;

public enum DMLQueryType {

    Insert(0, "insert"),
    Update(1, "update"),
    Delete(2, "delete"),
    Replace(3, "replace"),
    InsertIgnore(4, "insertIgnore"),
    InsertOnDupKey(5, "insertOnDupKey"),
    
    ;
    
    private static final DMLQueryType[] reverseMap;
    
    static{
        reverseMap = new DMLQueryType[DMLQueryType.values().length];
        for(DMLQueryType query : DMLQueryType.values()){
            if(reverseMap[query.getQueryIndex()] != null){
                throw new IllegalArgumentException("WRONG INDEX NUMBER");
            }
            reverseMap[query.getQueryIndex()] = query;
        }
    }
    
    public static DMLQueryType getQueryTypeByIndex(int i){
        return reverseMap[i];
    }
    
    private final int queryIndex;
    private final String propKey;
    
    private DMLQueryType(int queryIndex, String propKey){
        this.queryIndex = queryIndex;
        this.propKey = propKey;
    }
    
    public int getQueryIndex(){
        return this.queryIndex;
    }
    
    public String getPropKey(){
        return this.propKey;
    }
    
}
