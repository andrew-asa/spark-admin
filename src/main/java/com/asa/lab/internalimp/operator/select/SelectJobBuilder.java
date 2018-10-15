package com.asa.lab.internalimp.operator.select;

import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import com.asa.utils.ListUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class SelectJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        SelectOperator selectOperator = (SelectOperator) operator;
        List<SelectItem> selectItems = selectOperator.getItems();
        List<Column> scs = new ArrayList<>();
        List<String> tableList = new ArrayList<>();
        if (ListUtils.isNotEmpty(selectItems)) {
            for (SelectItem item : selectItems) {
                ListUtils.putIfAbsent(tableList, item.getTableName());
            }
            int tl = ListUtils.length(tableList);
            if (tl == 0) {
                return null;
            }
            // 单表
            if (tl == 1) {
                dataSet = SparkContentManager.getInstance().getDataset(tableList.get(0));
            }
            if (ListUtils.isNotEmpty(selectItems)) {
                for (SelectItem item : selectItems) {
                    Column column = dataSet.col(item.getColumnName());
                    scs.add(column);
                }
                dataSet = dataSet.select(scs.toArray(new Column[0]));
            }
            //dataSet = dataSet.select(scs.toArray(new Column[0]));
        }
        return dataSet;
    }
}
