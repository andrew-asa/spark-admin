package com.asa.lab.internalimp.datasource.empty;

import com.asa.lab.structure.datasource.Column;
import com.asa.lab.structure.datasource.DataSchema;
import com.asa.lab.structure.datasource.DataSet;
import com.asa.lab.structure.datasource.DataSource;
import com.asa.lab.structure.datasource.RowSet;
import com.asa.utils.StringUtils;

import java.io.Serializable;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
            public Object getObject(int row, int column) {

                return null;
            }

            @Override
            public int getColumnSize() {

                return 0;
            }

            @Override
            public int getRowSize() {

                return 0;
            }

            @Override
            public boolean getBoolean(int row, int column) {

                return false;
            }

            @Override
            public int getInt(int row, int column) {

                return 0;
            }

            @Override
            public long getLong(int row, int column) {

                return 0;
            }

            @Override
            public float getFloat(int row, int column) {

                return 0;
            }

            @Override
            public double getDouble(int row, int column) {

                return 0;
            }

            @Override
            public String getString(int row, int column) {

                return null;
            }

            @Override
            public Date getDate(int row, int column) {

                return null;
            }

            @Override
            public Timestamp getTimestamp(int row, int column) {

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
