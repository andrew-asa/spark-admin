package com.asa.lab.internalimp.operator.add.time;

import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.base.time.TimeInterval;
import com.asa.utils.AssistUtils;
import com.asa.utils.ComparatorUtils;

/**
 * @author andrew_asa
 * @date 2018/10/16.
 * 时间差
 * (endColumn - startColumn)/interval
 */
public class AddTimeDiffColumn extends AddNewColumnOperator {

    public static final String SUB_NAME = "addTimeDiffColumn";

    public static final String SYSTEM_COLUMN_NAME = "";

    private String startColumn;

    private String endColumn;

    private TimeInterval interval;

    public AddTimeDiffColumn(String startColumn, String endColumn, TimeInterval interval) {

        this.startColumn = startColumn;
        this.endColumn = endColumn;
        this.interval = interval;
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
    }

    public boolean isSystemColumn(String columnName) {

        return ComparatorUtils.equals(columnName, SYSTEM_COLUMN_NAME);
    }

    public String getStartColumn() {

        return startColumn;
    }

    public void setStartColumn(String startColumn) {

        this.startColumn = startColumn;
    }

    public String getEndColumn() {

        return endColumn;
    }

    public void setEndColumn(String endColumn) {

        this.endColumn = endColumn;
    }

    public TimeInterval getInterval() {

        return interval;
    }

    public void setInterval(TimeInterval interval) {

        this.interval = interval;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof AddTimeDiffColumn)) {
            return false;
        }

        AddTimeDiffColumn that = (AddTimeDiffColumn) o;
        return AssistUtils.equals(startColumn, that.startColumn) &&
                AssistUtils.equals(endColumn, that.endColumn) &&
                AssistUtils.equals(interval, that.interval);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(startColumn, endColumn, interval);
    }

    public boolean isStartTimeIsSystemTime() {

        return isSystemColumn(startColumn);
    }

    public boolean isEndTimeIsSystemTime() {

        return isSystemColumn(endColumn);
    }
}
