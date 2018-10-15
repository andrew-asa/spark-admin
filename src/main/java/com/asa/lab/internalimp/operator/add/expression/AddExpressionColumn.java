package com.asa.lab.internalimp.operator.add.expression;

import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 * 新增表达式类型列
 */
public class AddExpressionColumn extends AddNewColumnOperator {

    public static final String SUB_NAME = "addExpressionColumn";

    private String expression;

    public AddExpressionColumn(String expression) {

        this.expression = expression;
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
    }

    public String getExpression() {

        return expression;
    }

    public void setExpression(String expression) {

        this.expression = expression;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof AddExpressionColumn)) {
            return false;
        }

        AddExpressionColumn that = (AddExpressionColumn) o;
        return AssistUtils.equals(expression, that.expression) ;
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(expression);
    }
}
