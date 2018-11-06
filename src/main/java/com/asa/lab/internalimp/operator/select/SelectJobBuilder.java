package com.asa.lab.internalimp.operator.select;

import com.asa.lab.internalimp.datasource.empty.EmptyDataSource;
import com.asa.lab.internalimp.sql.datasource.DataSourceDataSetBuilder;
import com.asa.lab.structure.operator.ETLOperator;
import com.asa.lab.structure.operator.ETLOperatorJobBuilder;
import com.asa.lab.structure.service.etl.ETLJobBuilderContent;
import com.asa.lab.structure.service.spark.SparkContentManager;
import com.asa.utils.ListUtils;
import com.asa.utils.MapUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author andrew_asa
 * @date 2018/10/15.
 */
public class SelectJobBuilder implements ETLOperatorJobBuilder {

    @Override
    public Dataset<Row> build(Dataset<Row> dataSet, ETLOperator operator, ETLJobBuilderContent content) {

        SelectOperator selectOperator = (SelectOperator) operator;
        List<SelectItem> selectItems = selectOperator.getItems();
        Map<String, List<String>> tableFields = getTableFieldMap(selectItems);
        if (MapUtils.isNotEmptyMap(tableFields)) {
            // 单表
            if (tableFields.size() == 1) {
                dataSet = SparkContentManager.getInstance().getDataset(selectItems.get(0).getTableName());
            } else {

            }
        } else {
            // 没有选字段
            DataSourceDataSetBuilder dataSetBuilder = new DataSourceDataSetBuilder();
            dataSet = dataSetBuilder.build(new EmptyDataSource());
        }
        return dataSet;
    }

    private Map<String, List<String>> getTableFieldMap(List<SelectItem> items) {

        Map<String, List<String>> ret = new HashMap<>(16);
        if (ListUtils.isNotEmpty(items)) {
            items.forEach(item -> {
                List<String> fields = ret.get(item.getTableName());
                if (fields == null) {
                    fields = ListUtils.ensureNotNull(fields);
                    ret.put(item.getTableName(), fields);
                }
                fields.add(item.getColumnName());
            });
        }
        return ret;
    }
}
