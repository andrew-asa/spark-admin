package com.asa.lab.structure.service.etl;

import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.operator.ETLOperator;

import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/8.
 */
public interface ETLJobBuilder {

    /**
     * 输入一个data source
     * 一个系列operator
     * 输出一个data source
     *
     * @param content
     * @param ETLOperators
     * @return
     */
    DataSource build(ETLJobBuilderContent content, List<ETLOperator> ETLOperators);
}
