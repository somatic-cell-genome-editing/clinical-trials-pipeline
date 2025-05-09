package edu.mcw.scge.platform.model.platform;

import java.util.HashMap;
import java.util.Map;

public enum AliasType {
    PROPERNAME(1, "proper name"),
    PROPRIETARYNAME(2, "proprietary name");
    private final int code;
    private final String description;
    AliasType(int code, String description){
        this.code=code;
        this.description=description;

    }
    private static final Map<Integer, AliasType> type=new HashMap<>();
    static {
        for(AliasType aliasType:AliasType.values()){
            type.put(aliasType.code, aliasType);
        }
    }
    public int getCode(){
        return code;
    }
    public String getDescription(){return description;}
    public static AliasType getTypeByCode(int code){
        return type.get(code);
    }


}


