package com.eter.spark.data.dao.impl.spark.util;

import com.eter.spark.data.util.transform.reflect.SparkReversibleType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Created by rusifer on 3/26/17.
 */
public class ReflectRowToObject {

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

    public static Class reverseType(StructField field) {
        return SparkReversibleType.reverseFromSpark(field.dataType()).getJavaType();
    }

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
