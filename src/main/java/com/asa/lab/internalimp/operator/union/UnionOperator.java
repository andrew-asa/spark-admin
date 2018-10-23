package com.asa.lab.internalimp.operator.union;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class UnionOperator implements ETLOperator {

    public static final String NAME = "union";

    private List<UnionResultColumnItem> result;

    /**
     * 当前表合并依据
     */
    private UnionTableItem currentTable;

    /**
     * union 依赖的表
     */
    private List<UnionTableItem> unionTables;

    @Override
    public String getName() {

        return NAME;
    }

    public List<UnionResultColumnItem> getResult() {

        return result;
    }

    public void setResult(List<UnionResultColumnItem> result) {

        this.result = result;
    }

    public UnionTableItem getCurrentTable() {

        return currentTable;
    }

    public void setCurrentTable(UnionTableItem currentTable) {

        this.currentTable = currentTable;
    }

    public List<UnionTableItem> getUnionTables() {

        return unionTables;
    }

    public void setUnionTables(List<UnionTableItem> unionTables) {

        this.unionTables = unionTables;
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
