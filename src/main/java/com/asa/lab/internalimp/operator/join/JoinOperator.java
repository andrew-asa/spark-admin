package com.asa.lab.internalimp.operator.join;

import com.asa.lab.structure.operator.ETLOperator;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class JoinOperator implements ETLOperator {

    public static final String NAME = "join";

    private JoinType joinType;

    /**
     * 是否强制排序
     * 先左表然后再到右表
     */
    private boolean forceOrder;

    private List<JoinColumnItem> items;

    /**
     * 右表
     */
    private String rightTale;

    public JoinOperator(String rightTale, List<JoinColumnItem> items, JoinType joinType, boolean forceOrder) {

        this.joinType = joinType;
        this.forceOrder = forceOrder;
        this.items = items;
        this.rightTale = rightTale;
    }

    @Override
    public String getName() {

        return NAME;
    }

    public JoinType getJoinType() {

        return joinType;
    }

    public void setJoinType(JoinType joinType) {

        this.joinType = joinType;
    }

    public boolean isForceOrder() {

        return forceOrder;
    }

    public void setForceOrder(boolean forceOrder) {

        this.forceOrder = forceOrder;
    }

    public List<JoinColumnItem> getItems() {

        return items;
    }

    public void setItems(List<JoinColumnItem> items) {

        this.items = items;
    }

    public String getRightTale() {

        return rightTale;
    }

    public void setRightTale(String rightTale) {

        this.rightTale = rightTale;
    }
}
