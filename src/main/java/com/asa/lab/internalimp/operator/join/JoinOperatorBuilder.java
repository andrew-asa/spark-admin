package com.asa.lab.internalimp.operator.join;


import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class JoinOperatorBuilder {

    private List<JoinColumnItem> items;

    private String rightTale;

    private JoinType joinType;

    private boolean forceOrder;

    public JoinOperatorBuilder() {

        items = new ArrayList<>();
    }

    public JoinOperator build() {

        JoinOperator joinOperator = new JoinOperator(rightTale, items, joinType, forceOrder);
        return joinOperator;
    }

    public JoinOperatorBuilder addColumnItem(String leftColumn, String rightColumn, String resultColumn) {

        JoinColumnItem item = new JoinColumnItem(leftColumn, rightColumn, resultColumn);
        items.add(item);
        return this;
    }

    public JoinOperatorBuilder setJoinType(JoinType joinType) {

        this.joinType = joinType;
        return this;
    }

    public JoinOperatorBuilder setRightTale(String rightTale) {

        this.rightTale = rightTale;
        return this;
    }

    public JoinOperatorBuilder setForceOrder(boolean forceOrder) {

        this.forceOrder = forceOrder;
        return this;
    }
}
