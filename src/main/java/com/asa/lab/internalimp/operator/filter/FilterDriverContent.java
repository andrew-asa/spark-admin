package com.asa.lab.internalimp.operator.filter;


import com.asa.lab.internalimp.operator.filter.column.ColumnInFilterDriver;
import com.asa.lab.internalimp.operator.filter.column.ColumnInFilterOperator;
import com.asa.lab.internalimp.operator.filter.column.ColumnNotInFilterDriver;
import com.asa.lab.internalimp.operator.filter.column.ColumnNotInFilterOperator;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class FilterDriverContent {

    private static FilterDriverContent INSTANCE;

    private Map<String, FilterDriver> driverMap;

    private FilterDriverContent() {

        init();
    }

    public static FilterDriverContent getInstance() {

        if (INSTANCE == null) {
            synchronized (FilterDriverContent.class) {
                if (INSTANCE == null) {
                    INSTANCE = new FilterDriverContent();
                }
            }
        }
        return INSTANCE;
    }

    private void init() {

        driverMap = new HashMap<>();
    }

    /**
     * 设置默认的配置
     */
    public void setDefaultDriver() {

        addDriver(ColumnInFilterOperator.CONDITION_NAME, new ColumnInFilterDriver());
        addDriver(ColumnNotInFilterOperator.CONDITION_NAME, new ColumnNotInFilterDriver());
    }

    public FilterDriver getFilterDriver(FilterOperator operator) {

        return driverMap.get(operator.getConditionName());
    }

    public void addDriver(String conditionName, FilterDriver driver) {

        driverMap.put(conditionName, driver);
    }
}
