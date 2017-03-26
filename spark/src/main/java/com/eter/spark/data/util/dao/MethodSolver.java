package com.eter.spark.data.util.dao;

import javax.persistence.JoinColumn;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Util class for searching a method in {@link Class}
 */
public class MethodSolver<T> {

    /**
     * Return setter method.
     *
     * @param columnName name of column
     * @param type       java type what contain setter method
     * @param <T>        generic type of object
     * @return setter method
     */
    public static <T> Method getSetterMethod(String columnName, Class<T> type) {
        return getMethodWhatStartsWith("set", columnName, type);
    }

    /**
     * Return getter method.
     *
     * @param columnName name of column
     * @param type       java type what contain setter method
     * @param <T>        generic type of object
     * @return getter method
     */
    public static <T> Method getGetterMethod(String columnName, Class<T> type) {
        return getMethodWhatStartsWith("get", columnName, type);
    }

    /**
     * Return method in dependence of second argument.
     *
     * @param startsWith with what should start  method
     * @param columnName name of column
     * @param type       java type what contain method
     * @param <T>        generic type of object
     * @return method what contain second argument
     */
    public static <T> Method getMethodWhatStartsWith(String startsWith, String columnName, Class<T> type) {
        for (Method method : type.getDeclaredMethods()) {
            if (method.getName().startsWith(startsWith)) {
                if (method.getName().toLowerCase().contains(columnName.toLowerCase()))
                    return method;
            }
        }
        return null;
    }

    public static <T> List<Method> getRelationMethods(Class<T> type) {
        List<Method> methods = new ArrayList<>();
        for (Method method : type.getDeclaredMethods()) {
            if (method.isAnnotationPresent(JoinColumn.class)) {
                methods.add(method);
            }
        }
        return methods;
    }
}
