package com.asa.lab.internalimp.operator.join;

import com.asa.utils.ArrayUtils;
import com.asa.utils.ComparatorUtils;
import com.asa.utils.ListUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * @author andrew_asa
 * @date 2018/10/23.
 */
public class JoinColumnInfoBuilder {

    public static final String LEFT_COLUMN_PERFIEX = "left_";

    /**
     * 左表索引行
     */
    public static final String LEFT_INDEX_COLUMN = "__left_index_column__";

    public static final String RIGHT_COLUMN_PERFIEX = "right_";

    /**
     * 右表索引行
     */
    public static final String RIGHT_INDEX_COLUMN = "__right_index_column__";


    public JoinColumnInfoBuilder() {

    }

    public List<Column> buildColumns(Dataset<Row> result, List<JoinColumnItem> joinColumnItems) {

        List<Column> ret = new ArrayList<>();
        StructField[] fs = result.schema().fields();
        List<StructField> fields = ArrayUtils.arrayToList(fs);
        for (JoinColumnItem columnItem : joinColumnItems) {
            String lc = columnItem.getLeftColumn();
            String mlc = mapLeftColumnName(lc);
            String rcn = columnItem.getResultColumn();
            fields = removeColumn(fields, mlc);
            ret.add(result.col(mlc).as(rcn));

            String rc = columnItem.getRightColumn();
            String mrc = mapRightColumnName(rc);
            fields = removeColumn(fields, mrc);
        }
        for (StructField field : fields) {
            ret.add(result.col(field.name()));
        }
        return ret;
    }

    public List<StructField> removeColumn(List<StructField> fields, String columnName) {

        List<StructField> ret = new ArrayList<>();
        for (StructField field : fields) {
            if (!ComparatorUtils.equals(field.name(), columnName)) {
                ret.add(field);
            }
        }
        return ret;
    }

    public static Dataset<Row> rebuildResultColumn(Dataset<Row> result, boolean forceOrder, List<JoinColumnItem> joinColumnItems) {

        if (forceOrder) {
            result = result.orderBy(result.col(LEFT_INDEX_COLUMN), result.col(RIGHT_INDEX_COLUMN));
        }
        JoinColumnInfoBuilder builderInfo = new JoinColumnInfoBuilder();
        List<Column> columns = builderInfo.buildColumns(result, joinColumnItems);
        result = result.select(columns.toArray(new Column[0]));
        if (forceOrder) {
            result = result.drop(result.col(LEFT_INDEX_COLUMN)).drop(result.col(RIGHT_INDEX_COLUMN));
        }
        return result;
    }

    public static Column buildJoinOnCondition(Dataset<Row> left, Dataset<Row> right, List<JoinColumnItem> joinColumnItems) {

        int columnCount = ListUtils.length(joinColumnItems);
        Column column = null;
        for (int i = 0; i < columnCount; i++) {
            JoinColumnItem item = joinColumnItems.get(i);
            String leftColumnName = item.getLeftColumn();
            String rightColumnName = item.getRightColumn();
            Column leftColumn = findLeftColumn(left, leftColumnName);
            Column rightColumn = findRightColumn(right, rightColumnName);
            Column newCol = leftColumn.equalTo(rightColumn);
            if (column == null) {
                column = newCol;
            } else {
                column = column.and(newCol);
            }
        }
        return column;
    }

    private static Column findLeftColumn(Dataset<Row> dataFrame, String originalFieldName) {

        String name = mapLeftColumnName(originalFieldName);
        return dataFrame.apply(name);
    }

    private static Column findRightColumn(Dataset<Row> dataFrame, String originalFieldName) {

        String name = mapRightColumnName(originalFieldName);
        return dataFrame.apply(name);
    }

    public static Dataset<Row> renameRightColumnPrefix(Dataset<Row> dataFrame) {

        return renameColumnPrefix(dataFrame, JoinColumnInfoBuilder::mapRightColumnName);
    }

    public static Dataset<Row> renameLeftColumnPrefix(Dataset<Row> dataFrame) {

        return renameColumnPrefix(dataFrame, JoinColumnInfoBuilder::mapLeftColumnName);
    }


    public static Dataset<Row> renameColumnPrefix(Dataset<Row> dataFrame, Function<String, String> fieldNameMapping) {

        String[] fields = dataFrame.columns();
        Column[] columns = Arrays.stream(fields)
                .map(f -> dataFrame.apply(f).name(fieldNameMapping.apply(f)))
                .toArray(Column[]::new);
        return dataFrame.select(columns);
    }

    private static String mapLeftColumnName(String fieldName) {

        return RIGHT_COLUMN_PERFIEX + fieldName;
    }

    private static String mapRightColumnName(String fieldName) {

        return RIGHT_COLUMN_PERFIEX + fieldName;
    }
}
