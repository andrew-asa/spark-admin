package com.asa.lab.internalimp.operator.add.cumulative;

import com.asa.lab.internalimp.operator.add.AbstractAddSummaryColumn;
import com.asa.lab.internalimp.operator.add.AddNewColumnOperator;
import com.asa.lab.structure.base.summary.SummaryType;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/17.
 * 新增求汇总列-累计值
 */
public class AddCumulativeColumn extends AbstractAddSummaryColumn {

    public static final String SUB_NAME = "addCumulativeColumn";

    public AddCumulativeColumn(String summaryColumn, List<String> groupColumns, boolean inGroup) {

        super(summaryColumn, groupColumns, inGroup);
    }

    @Override
    public String getSubName() {

        return SUB_NAME;
    }
}
