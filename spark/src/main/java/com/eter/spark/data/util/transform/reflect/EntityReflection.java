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
            SparkReversibleType dataType = getFieldType(field);
            String fieldName = field.getName();

            if (dataType == SparkReversibleType.REFERENCETYPE) {
                    fieldName += "id";
            }

            if (dataType != SparkReversibleType.UNKNOWN) {
                structFields.add(new StructField(fieldName, dataType.getSparkType(), true, Metadata.empty()));
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

    private static SparkReversibleType getFieldType(Field field) {
        Class fieldType = field.getType();
        Optional<SparkReversibleType> reversibleType = Optional.ofNullable(SparkReversibleType.reverseFromJavaType(fieldType));

        return reversibleType.orElseGet(() -> SparkReversibleType.UNKNOWN);
    }

}
