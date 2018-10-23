package com.asa.lab.internalimp.operator.join;

import com.asa.utils.AssistUtils;

/**
 * @author andrew_asa
 * @date 2018/10/23.
 */
public class JoinColumnItem {

    /**
     * 左表字段
     */
    private String leftColumn;

    /**
     * 右边字段
     */
    private String rightColumn;

    /**
     * 结果字段
     */
    private String resultColumn;

    public JoinColumnItem(String leftColumn, String rightColumn, String resultColumn) {

        this.leftColumn = leftColumn;
        this.rightColumn = rightColumn;
        this.resultColumn = resultColumn;
    }

    public String getLeftColumn() {

        return leftColumn;
    }

    public void setLeftColumn(String leftColumn) {

        this.leftColumn = leftColumn;
    }

    public String getRightColumn() {

        return rightColumn;
    }

    public void setRightColumn(String rightColumn) {

        this.rightColumn = rightColumn;
    }

    public String getResultColumn() {

        return resultColumn;
    }

    public void setResultColumn(String resultColumn) {

        this.resultColumn = resultColumn;
    }


    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof JoinColumnItem)) {
            return false;
        }

        JoinColumnItem that = (JoinColumnItem) o;
        return AssistUtils.equals(leftColumn, that.leftColumn) &&
                AssistUtils.equals(rightColumn, that.rightColumn) &&
                AssistUtils.equals(resultColumn, that.resultColumn);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(leftColumn, rightColumn, resultColumn);
    }
}
