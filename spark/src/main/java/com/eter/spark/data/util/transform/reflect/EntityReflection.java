package com.eter.spark.data.util.transform.reflect;


import org.apache.spark.sql.api.java.DataType;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Transform entity all supported typed fields to {@link StructType} (spark-sql schema).
 * For example:
 * class C {
 * Integer id;
 * String sameString;
 * }
 * transform to:
 * StructType [
 * StructField { id : Integer }
 * StructField {sameString : String}
 * ]
 * Supported type: {@link Integer}, {@link Double}, {@link Boolean},
 * {@link String}, {@link Enum} (only as string value).
 */
public class EntityReflection {
    /**
     * Transform argument class to {@link StructType} what is used for spark-sql as schema definition.
     * In case, what type of field is custom user type - will interpret as reference type (relation)
     * and transform field to fieldName + 'id'.
     *
     * @param tClass class of entity to reflect
     * @param <T>    type of entity
     * @return {@link StructType} what is interpret by spark-sql as schema definition
     */
    public static <T> StructType reflectEntityToSparkSchema(Class<T> tClass) {
        Field[] fields = tClass.getDeclaredFields();
        List<StructField> structFields = new ArrayList<StructField>();

        for (Field field : fields) {
            DataType dataType = getFieldType(field);
            String fieldName = field.getName();

            if (dataType == null) {
                if (isUserCustomType(field)) {
                    fieldName += "Id";
                    dataType = DataType.IntegerType;
                }
            }

            if (dataType != null) {
                structFields.add(StructType.createStructField(fieldName, dataType, true));
            }
        }

        return DataType.createStructType(structFields);
    }

    /**
     * Check field type. If field type is not numeric/boolean or enum -
     * will return null
     *
     * @param field {@link Field} to check
     * @return {@link DataType} what represent type of {@link Field}
     */

    private static DataType getFieldType(Field field) {
        Class fieldType = field.getType();
        if (fieldType.isAssignableFrom(Long.class))
            return DataType.IntegerType;
        else if (fieldType.isAssignableFrom(Double.class))
            return DataType.DoubleType;
        else if (fieldType.isAssignableFrom(Boolean.class))
            return DataType.BooleanType;
        else if (fieldType.isAssignableFrom(String.class))
            return DataType.StringType;
        else if (fieldType.isAssignableFrom(Integer.class))
            return DataType.IntegerType;
        else if (fieldType.isEnum())
            return DataType.StringType;

        return null;
    }

    /**
     * Check if field type is user custom type.
     *
     * @param field - field to check
     * @return true if field type is user custom type, or false if is not
     */
    private static boolean isUserCustomType(Field field) {
        return !field.getType().isPrimitive() && !field.getType().getName().startsWith("java.lang");

    }
}
