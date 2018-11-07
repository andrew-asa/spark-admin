package com.asa.lab.internalimp.datasource.relation;

import com.asa.lab.internalimp.datasource.DataSchemaHelper;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.relation.Relation;
import com.asa.lab.structure.datasource.relation.RelationIndexMapping;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/11/7.
 */
public class RelationColumnInfo {

    private Relation relation;

    private String foreignTable;

    private List<String> columns = new ArrayList<>();

    private DataSource foreignDataSource;

    private DataSchemaHelper schemaHelper;

    private RelationIndexMapping relationIndexMapping;

    public Relation getRelation() {

        return relation;
    }

    public void setRelation(Relation relation) {

        this.relation = relation;
    }

    public String getForeignTable() {

        return foreignTable;
    }

    public void setForeignTable(String foreignTable) {

        this.foreignTable = foreignTable;
    }

    public List<String> getColumns() {

        return columns;
    }

    public void setColumns(List<String> columns) {

        this.columns = columns;
    }

    public DataSource getForeignDataSource() {

        return foreignDataSource;
    }

    public void setForeignDataSource(DataSource foreignDataSource) {

        this.foreignDataSource = foreignDataSource;
    }

    public DataSchemaHelper getSchemaHelper() {

        return schemaHelper;
    }

    public void setSchemaHelper(DataSchemaHelper schemaHelper) {

        this.schemaHelper = schemaHelper;
    }

    public void addColumn(String columnName) {

        columns.add(columnName);
    }

    public RelationIndexMapping getRelationIndexMapping() {

        return relationIndexMapping;
    }

    public void setRelationIndexMapping(RelationIndexMapping relationIndexMapping) {

        this.relationIndexMapping = relationIndexMapping;
    }
}
