package com.asa.lab.internalimp.datasource.relation;

import com.asa.lab.structure.datasource.relation.Relation;
import com.asa.utils.AssistUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 */
public class DefaultRelation implements Relation {

    private String primaryTable;

    private String foreignTable;

    private List<String> primaryFields;

    private List<String> foreignFields;

    public DefaultRelation(String primaryTable, String foreignTable, List<String> primaryFields, List<String> foreignFields) {

        this.primaryTable = primaryTable;
        this.foreignTable = foreignTable;
        this.primaryFields = primaryFields;
        this.foreignFields = foreignFields;
    }

    @Override
    public String getPrimaryTable() {

        return primaryTable;
    }

    @Override
    public List<String> getPrimaryFields() {

        return primaryFields;
    }

    @Override
    public String getForeignTable() {

        return foreignTable;
    }

    @Override
    public List<String> getForeignFields() {

        return foreignFields;
    }

    public void setPrimaryTable(String primaryTable) {

        this.primaryTable = primaryTable;
    }

    public void setForeignTable(String foreignTable) {

        this.foreignTable = foreignTable;
    }

    public void setPrimaryFields(List<String> primaryFields) {

        this.primaryFields = primaryFields;
    }

    public void setForeignFields(List<String> foreignFields) {

        this.foreignFields = foreignFields;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof DefaultRelation)) {
            return false;
        }

        DefaultRelation that = (DefaultRelation) o;
        return AssistUtils.equals(primaryTable, that.primaryTable) &&
                AssistUtils.equals(foreignTable, that.foreignTable) &&
                AssistUtils.equals(primaryFields, that.primaryFields) &&
                AssistUtils.equals(foreignFields, that.foreignFields);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(primaryTable, foreignTable, primaryFields, foreignFields);
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
