package com.asa.lab.internalimp.datasource.relation;

import com.asa.utils.ComparatorUtils;
import com.asa.utils.ListUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 */
public class RelationBuilderHelperTest {

    @Test
    public void buildSimpleRelation() throws Exception {

        Relation relation = RelationBuilderHelper.buildSimpleRelation("table1:::field1:_:table2:::field2");
        Assert.assertEquals(relation.getPrimaryTable(), "table1");
        Assert.assertEquals(relation.getForeignTable(), "table2");
        List<String> primaryFields = relation.getPrimaryFields();
        List<String> foreignFields = relation.getForeignFields();
        ComparatorUtils.equals(primaryFields, ListUtils.arrayToList("field1"));
        ComparatorUtils.equals(foreignFields, ListUtils.arrayToList("field2"));
    }

}