package com.asa.lab.internalimp.datasource.empty;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.RowSet;
import com.asa.utils.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/11/6.
 */
public class EmptyDataSource implements DataSource, Serializable {

    public static final String URLSCHMA = "empty";

    @Override
    public String getName() {

        return StringUtils.EMPTY;
    }

    @Override
    public void setName(String name) {

    }

    @Override
    public DataSet getDataSet() {

        return new DataSet() {

            @Override
            public List<RowSet> getDataList() {

                return new ArrayList<>();
            }

            @Override
            public int size() {

                return 0;
            }

            @Override
            public Object getObject(int row, int column) {

                return null;
            }
        };
    }

    @Override
    public DataSchema getSchema() {

        return new DataSchema() {

            @Override
            public Column[] getColumns() {

                return new Column[0];
            }

            @Override
            public void setColumns(Column[] columns) {

            }
        };
    }

    @Override
    public void setSchema(DataSchema schema) {

    }

    @Override
    public String getURIScheme() {

        return URLSCHMA;
    }
}
