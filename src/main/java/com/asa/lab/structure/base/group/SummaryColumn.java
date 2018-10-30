package com.asa.lab.structure.base.group;

import com.asa.lab.structure.base.summary.SummaryType;

import java.io.Serializable;

/**
 * @author andrew_asa
 * @date 2018/10/29.
 * 汇总列
 */
public class SummaryColumn extends AbstractGroupSummaryColumn implements Serializable {

    private SummaryType summaryType;

    public SummaryColumn(String source, SummaryType summaryType, String displayName) {

        setColumnSourceName(source);
        this.summaryType = summaryType;
        setDisplayName(displayName);
    }

    public SummaryType getSummaryType() {

        return summaryType;
    }

    public void setSummaryType(SummaryType summaryType) {

        this.summaryType = summaryType;
    }
}
