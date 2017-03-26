package com.eter.spark.data.util.dao;

import com.eter.spark.data.util.transform.reflect.SparkReversibleType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Transform and instantiate a entity object from {@link Dataset} {@link Row} object.
 */
public class ReflectRowToObject {


    /**
     * Reflect row to a instance of type indicated by {@link Class}
     *
     * @param row  Row value to reflect
     * @param type object instance type
     * @return instance of object
     * @throws InstantiationException    when can't create instance of object
     * @throws NoSuchMethodException     when can't find setter method
     * @throws IllegalAccessException    when is incorrect access method
     * @throws InvocationTargetException when is not an implicit public constructor of object
     */
    public static <T> T reflect(Row row, Class<T> type) throws InstantiationException, NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        T object = type.newInstance();
        for (StructField field : row.schema().fields()) {
            String columnName = field.name();
            if (!columnName.equals("id") && columnName.contains("id")) {
                //todo: relation resolver
            }

            Optional<Method> setter = Optional.ofNullable(getSetterMethod(columnName, type));
            setter.ifPresent((method) -> {
                try {
                    method.invoke(object, row.get(row.fieldIndex(field.name())));
                } catch (IllegalAccessException | InvocationTargetException e) {

                }
            });
        }

        return object;
    }

    /**
     * Transform from Apache Spark type to java type.
     *
     * @param field {@link StructField} what contain Apache Spark Type
     * @return java type
     */
    public static Class reverseType(StructField field) {
        return SparkReversibleType.reverseFromSpark(field.dataType()).getJavaType();
    }


    /**
     * Find setter method based on column name.
     *
     * @param columnName name of column
     * @param type       java type what contain setter method
     * @param <T>        object type
     * @return return setter method
     */
    public static <T> Method getSetterMethod(String columnName, Class<T> type) {
        for (Method method : type.getDeclaredMethods()) {
            if (method.getName().contains("set")) {
                if (method.getName().toLowerCase().contains(columnName.toLowerCase()))
                    return method;
            }
        }
        return null;
    }
}
