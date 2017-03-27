package com.eter.spark.data.util.transform.reflect;

import com.eter.spark.data.util.transform.reflect.MethodSolver;
import com.eter.spark.data.util.transform.reflect.SparkReversibleType;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

/**
 * Transform and instantiate an entity object from {@link org.apache.spark.sql.Dataset} {@link Row} object.
 */
public class ReflectRowToObject {


    /**
     * Reflect row to a instance of type indicated by {@link Class}
     *
     * @param row  Row value to reflect
     * @param type object instance type
     * @param <T>  object type
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
                continue;
            }

            Optional<Method> setter = Optional.ofNullable(MethodSolver.getSetterMethod(columnName, type));

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


}
