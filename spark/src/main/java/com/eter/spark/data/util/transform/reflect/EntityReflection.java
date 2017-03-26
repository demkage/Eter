package com.eter.spark.data.util.transform.reflect;


import org.apache.spark.sql.types.*;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

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
                    dataType = DataTypes.IntegerType;
                }
            }

            if (dataType != null) {
                structFields.add(new StructField(fieldName, dataType, true, Metadata.empty()));
            }
        }

        return DataTypes.createStructType(structFields);
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
        Optional<SparkReversibleType> reversibleType = Optional.ofNullable(SparkReversibleType.reverseFromJavaType(fieldType));
        return reversibleType.orElseGet(() -> SparkReversibleType.INTEGER).getSparkType();
//        if (fieldType.isAssignableFrom(Long.class))
//            return DataTypes.LongType;
//        else if (fieldType.isAssignableFrom(Double.class))
//            return DataTypes.DoubleType;
//        else if (fieldType.isAssignableFrom(Float.class))
//            return DataTypes.FloatType;
//        else if (fieldType.isAssignableFrom(Boolean.class))
//            return DataTypes.BooleanType;
//        else if (fieldType.isAssignableFrom(String.class))
//            return DataTypes.StringType;
//        else if (fieldType.isAssignableFrom(Integer.class))
//            return DataTypes.IntegerType;
//        else if (fieldType.isEnum())
//            return DataTypes.StringType;

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
