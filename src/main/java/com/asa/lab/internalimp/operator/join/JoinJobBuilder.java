package com.asa.lab.internalimp.operator.join;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import com.asa.utils.ListUtils;
import com.asa.utils.StringUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.StructField;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class JoinJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        JoinOperator joinOperator = (JoinOperator) operator;
        JoinType joinType = joinOperator.getJoinType();
        List<JoinColumnItem> joinColumnItems = joinOperator.getItems();
        String rightTable = joinOperator.getRightTale();
        boolean forceOrder = joinOperator.isForceOrder();
        if (ListUtils.isNotEmpty(joinColumnItems) && StringUtils.isNotEmpty(rightTable)) {
            Dataset<Row> right = SparkContentManager.getInstance().getDataset(rightTable);
            Dataset<Row> left = JoinColumnInfoBuilder.renameLeftColumnPrefix(dataSet);
            right = JoinColumnInfoBuilder.renameRightColumnPrefix(right);
            // 强制排序
            if (forceOrder) {
                left = left.withColumn(JoinColumnInfoBuilder.LEFT_INDEX_COLUMN, functions.monotonically_increasing_id());
                right = right.withColumn(JoinColumnInfoBuilder.RIGHT_INDEX_COLUMN, functions.monotonically_increasing_id());
            }
            Column condition = JoinColumnInfoBuilder.buildJoinOnCondition(left, right, joinColumnItems);
            Dataset<Row> result = left.join(right, condition, joinType.getName());
            if (forceOrder) {
                result = result.sort(result.col(JoinColumnInfoBuilder.LEFT_INDEX_COLUMN), result.col(JoinColumnInfoBuilder.RIGHT_INDEX_COLUMN));
            }
            return JoinColumnInfoBuilder.rebuildResultColumn(result, forceOrder, joinColumnItems);
        }
        return dataSet;
    }


}
