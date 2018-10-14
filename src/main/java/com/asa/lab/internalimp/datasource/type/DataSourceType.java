package com.asa.lab.internalimp.datasource.type;

/**
 * @author andrew_asa
 * @date 2018/10/9.
 * 数据源类型
 */
public enum DataSourceType {

    NONE("0"),
    MEMORY("1"),
    DB("2"),;

    private String type;

    DataSourceType(String type) {

        this.type = type;
    }

    public String getType() {

        return type;
    }

    public static DataSourceType create(String type) {

        for (DataSourceType sourceType : DataSourceType.values()) {
            if (sourceType.getType().equals(type)) {
                return sourceType;
            }
        }
        return NONE;
    }
}
