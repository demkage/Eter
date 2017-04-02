package com.eter.spark.data.util.dao.spark;

import com.eter.spark.data.util.transform.reflect.MethodSolver;

import javax.persistence.JoinColumn;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.*;

/**
 * Created by rusifer on 4/1/17.
 */
public class JoinSelectorBuilder {
    private Class forType;
    private Map<Class, List<String>> joinColumns;


    private JoinSelectorBuilder(Class forType) {
        this.forType = forType;
        this.joinColumns = new HashMap<>();
    }

    public static JoinSelectorBuilder createBuilder(Class type) {
        return new JoinSelectorBuilder(type);
    }

    public JoinSelectorBuilder forType(Class type) {
        this.forType = type;
        return this;
    }

    public JoinSelectorBuilder join(Class type) {
        if (!joinColumns.containsKey(type))
            joinColumns.put(type, new ArrayList<>());

        return this;
    }

    public JoinSelectorBuilder select(Class type, String column) {
        List<String> columns = joinColumns.get(type);

        if (columns == null) {
            columns = new ArrayList<>();
            joinColumns.put(type, columns);
        }

        columns.add(column);

        return this;
    }

    public JoinSelectorBuilder select(Class type, Collection<String> columns) {
        List<String> columnList = joinColumns.get(type);

        if (columnList == null) {
            columnList = new ArrayList<>(columns);
            joinColumns.put(type, columnList);
        } else {
            columnList.addAll(columns);
        }

        return this;
    }

    public JoinSelectorBuilder selectAll(String type) {
        List<String> selectColumns = new ArrayList<>();
        Method method = MethodSolver.getGetterMethod(type, forType);
        Class referenceType = method.getReturnType();

        for (Field field : referenceType.getDeclaredFields()) {
            if (field.getType().getName().startsWith("java.lang"))
                selectColumns.add(field.getType().getSimpleName().toLowerCase());
        }

        return this;
    }

    public JoinSelector build() throws NoSuchMethodException {
        Iterator<Map.Entry<Class, List<String>>> iterator = joinColumns.entrySet().iterator();

        while (iterator.hasNext()) {
            Map.Entry<Class, List<String>> entry = iterator.next();
            if (entry.getKey() != forType) {
                Method joinMethod = MethodSolver.getGetterMethod(entry.getKey().getSimpleName().toLowerCase(), forType);

                if (joinMethod == null && hasJoinColumn(joinMethod)) {
                    throw new NoSuchMethodException("Getter method for '" + entry.getKey() + "' doesn't exist");
                }
            }
        }

        return new JoinSelector(joinColumns);
    }

    private boolean hasJoinColumn(Method method) {
        JoinColumn joinColumn = method.getDeclaredAnnotation(JoinColumn.class);

        return joinColumn != null;
    }
}
