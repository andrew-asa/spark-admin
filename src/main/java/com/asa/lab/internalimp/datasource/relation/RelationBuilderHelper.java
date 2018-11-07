package com.asa.lab.internalimp.datasource.relation;

import com.asa.lab.structure.datasource.relation.Relation;
import com.asa.utils.ArrayUtils;
import com.asa.utils.ListUtils;
import com.asa.utils.StringUtils;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 */
public class RelationBuilderHelper {

    public static final String FIELD_SEPERATOR = ":::";

    public static final String TABLE_SEPERATOR = ":_:";

    /**
     * 构建简单关联
     *
     * @param relationStr table1:::field1:_:table2:::field2
     *                    table1为大表
     *                    table2为小表
     * @return
     */
    public static Relation buildSimpleRelation(String relationStr) {


        if (StringUtils.isEmpty(relationStr)) {
            throw new RuntimeException(
                    StringUtils.messageFormat(
                            "relationStr {} is null",
                            relationStr)
            );
        }
        String[] tables = relationStr.split(TABLE_SEPERATOR);
        if (ArrayUtils.length(tables) != 2) {
            if (StringUtils.isEmpty(relationStr)) {
                throw new RuntimeException(
                        StringUtils.messageFormat(
                                "relation Str {} no contain 2 table",
                                relationStr)
                );
            }
        }
        String primaryTableStr = tables[0];
        String foreignTableStr = tables[1];
        String[] primaryTables = primaryTableStr.split(FIELD_SEPERATOR);
        String[] foreignTables = foreignTableStr.split(FIELD_SEPERATOR);

        if (ArrayUtils.length(primaryTables) != 2 || ArrayUtils.length(foreignTables) != 2) {
            if (StringUtils.isEmpty(relationStr)) {
                throw new RuntimeException(
                        StringUtils.messageFormat(
                                "relation Str {} no available",
                                relationStr)
                );
            }
        }

        String primaryTable = primaryTables[0];
        String foreignTable = foreignTables[0];
        List<String> primaryFields = ListUtils.arrayToList(primaryTables[1]);
        List<String> foreignFields = ListUtils.arrayToList(foreignTables[1]);
        DefaultRelation defaultRelation = new DefaultRelation(primaryTable, foreignTable, primaryFields, foreignFields);
        return defaultRelation;
    }
}
