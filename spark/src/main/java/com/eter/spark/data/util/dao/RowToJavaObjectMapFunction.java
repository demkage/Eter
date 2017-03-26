package com.eter.spark.data.util.dao;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.lang.reflect.InvocationTargetException;

/**
 * Used for transform a {@link Row} in {@link Dataset} to user define type.
 */
public class RowToJavaObjectMapFunction<U> implements MapFunction<Row, U> {
    private static final long serialVersionUID = -3959017028910340287L;
    private Class<U> type;

    /**
     * Create new object what will transform {@link Row} to a new instance of {@link Class} type.
     *
     * @param type define object type
     */
    public RowToJavaObjectMapFunction(Class<U> type) {
        this.type = type;
    }

    /**
     * Reflect row to a instance of type indicated by {@link Class}
     *
     * @param value Row value to reflect
     * @return instance of object
     * @throws InstantiationException    when can't create instance of object
     * @throws NoSuchMethodException     when can't find setter method
     * @throws IllegalAccessException    when is incorrect access method
     * @throws InvocationTargetException when is not an implicit public constructor of object
     */
    @Override
    public U call(Row value) throws InstantiationException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        return ReflectRowToObject.reflect(value, type);

    }

}
