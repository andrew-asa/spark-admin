package com.asa.lab.internalimp.operator.add.custom;

import com.asa.lab.structure.base.group.custom.CustomGroupColumn;
import com.asa.lab.structure.base.group.custom.CustomGroupItem;
import com.asa.lab.structure.base.group.GroupColumn;
import com.asa.lab.structure.base.group.GroupType;
import com.asa.utils.ListUtils;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 */
public class CustomColumnRowIterator implements Iterator<Row>, Serializable {

    private Iterator<Row> rowIterator;

    private AddCustomColumn operator;

    private StructType structType;

    private String customColumn;

    private GroupColumn groupInfo;

    private Map<Object, Object> cache;

    public CustomColumnRowIterator(Iterator<Row> rowIterator, AddCustomColumn operator, StructType structType) {

        this.rowIterator = rowIterator;
        this.operator = operator;
        this.structType = structType;
        init();
    }

    private void init() {

        groupInfo = operator.getGroupInfo();
        customColumn = groupInfo.getColumnSourceName();
        cache = new HashMap<>();
    }

    @Override
    public boolean hasNext() {

        return rowIterator.hasNext();
    }

    @Override
    public Row next() {

        Row row = rowIterator.next();
        int size = row.size();
        Object[] replaceValues = new Object[size + 1];
        if (size > 0) {
            for (int i = size - 1; i >= 0; i--) {
                replaceValues[i] = row.apply(i);
            }
            Object o = row.getAs(customColumn);
            replaceValues[size] = getGroupValue(o);
        }
        row = new GenericRowWithSchema(replaceValues, structType);
        return row;
    }

    public Iterator<Row> getRowIterator() {

        return rowIterator;
    }

    public void setRowIterator(Iterator<Row> rowIterator) {

        this.rowIterator = rowIterator;
    }

    public AddCustomColumn getOperator() {

        return operator;
    }

    public void setOperator(AddCustomColumn operator) {

        this.operator = operator;
    }

    private Object getGroupValue(Object value) {

        GroupType groupType = groupInfo.getGroupType();
        if (GroupType.CUSTOM.equals(groupType)) {
            CustomGroupColumn column = (CustomGroupColumn) groupInfo;
            return getCustomValue(value, column);
        }
        return value;
    }

    private Object getCustomValue(Object value, CustomGroupColumn column) {

        if (value != null) {
            if (cache.containsKey(value)) {
                return cache.get(value);
            }
            List<CustomGroupItem> customGroupItems = column.getCustom();
            if (ListUtils.isNotEmpty(customGroupItems)) {
                for (CustomGroupItem item : customGroupItems) {
                    List<Object> values = item.getValues();
                    if (ListUtils.contain(values, value)) {
                        cache.put(value, item.getDisplayName());
                        return item.getDisplayName();
                    }
                }
            }
            if (column.isUserOther()) {
                return column.getOtherGroupName();
            }
        }
        return value;
    }
}
