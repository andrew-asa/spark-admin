package com.asa.lab.internalimp.operator.add.custom;

import com.asa.utils.AssistUtils;

import java.io.Serializable;
import java.util.List;

/**
 * @author andrew_asa
 * @date 2018/10/18.
 */
public class CustomGroupItem implements Serializable {

    private String displayName;

    private List<Object> values;

    public CustomGroupItem(String displayName, List<Object> values) {

        this.displayName = displayName;
        this.values = values;
    }

    public String getDisplayName() {

        return displayName;
    }

    public void setDisplayName(String displayName) {

        this.displayName = displayName;
    }

    public List<Object> getValues() {

        return values;
    }

    public void setValues(List<Object> values) {

        this.values = values;
    }

    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }
        if (!(o instanceof CustomGroupItem)) {
            return false;
        }

        CustomGroupItem that = (CustomGroupItem) o;
        return AssistUtils.equals(displayName, that.displayName) &&
                AssistUtils.equals(values, that.values);
    }

    @Override
    public int hashCode() {

        return AssistUtils.hashCode(displayName, values);
    }

    @Override
    public String toString() {

        return AssistUtils.toString(this);
    }
}
