package com.eter.spark.data.util.dao.spark;

import java.util.*;

/**
 * Created by rusifer on 4/1/17.
 */
public class JoinSelector {
    private Map<Class, List<String>> joinColumns;

    public JoinSelector() {
    }

    public JoinSelector(Map<Class, List<String>> joinColumns) {
        this.joinColumns = joinColumns;
    }

    public Map<Class, List<String>> getJoinColumns() {
        return joinColumns;
    }

    public void setJoinColumns(Map<Class, List<String>> joinColumns) {
        this.joinColumns = joinColumns;
    }

    public void selectOn(Class type, String column) {
        if (joinColumns == null)
            joinColumns = new HashMap<>();

        List<String> columns = joinColumns.get(type);

        if (columns == null) {
            columns = new ArrayList<>();
            joinColumns.put(type, columns);
        }

        columns.add(column);
    }

    public void selectOn(Class type, Collection<String> columns) {
        if (joinColumns == null)
            joinColumns = new HashMap<>();

        List<String> columnsList = joinColumns.get(type);

        if (columnsList == null)
            joinColumns.put(type, new ArrayList<>(columns));
        else
            columnsList.addAll(columns);

    }

    public Collection<String> getColumns(Class type) {
        return joinColumns.get(type);
    }

    public Collection<Class> getTypes() {
        return joinColumns.keySet();
    }
}
