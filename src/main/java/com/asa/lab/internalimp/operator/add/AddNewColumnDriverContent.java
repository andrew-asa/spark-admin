package com.asa.lab.internalimp.operator.add;

import com.asa.lab.internalimp.operator.add.expression.AddExpressionColumn;
import com.asa.lab.internalimp.operator.add.expression.AddExpressionDriver;

import java.util.HashMap;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class AddNewColumnDriverContent {

    private static AddNewColumnDriverContent INSTANCE;

    private Map<String, AddNewColumnDriver> driverMap;

    private AddNewColumnDriverContent() {

        init();
    }

    public static AddNewColumnDriverContent getInstance() {

        if (INSTANCE == null) {
            synchronized (AddNewColumnDriverContent.class) {
                if (INSTANCE == null) {
                    INSTANCE = new AddNewColumnDriverContent();
                }
            }
        }
        return INSTANCE;
    }

    private void init() {

        driverMap = new HashMap<>();
        setDefaultDriver();
    }

    /**
     * 设置默认的配置
     */
    public void setDefaultDriver() {

        addDriver(AddExpressionColumn.SUB_NAME, new AddExpressionDriver());
    }

    public AddNewColumnDriver getAddNewColumnDriver(AddNewColumnOperator operator) {

        return driverMap.get(operator.getSubName());
    }

    public void addDriver(String conditionName, AddNewColumnDriver driver) {

        driverMap.put(conditionName, driver);
    }
}
