package com.eter.spark.data.util.transform.reflect;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Used for identify java/spark type and convert to his alternatives to spark/java.
 */
public enum SparkReversibleType {
    INTEGER(Integer.class, DataTypes.IntegerType),
    DOUBLE(Double.class, DataTypes.DoubleType),
    STRING(String.class, DataTypes.StringType),
    FLOAT(Float.class, DataTypes.FloatType),
    LONG(Long.class, DataTypes.LongType),
    SHORT(Short.class, DataTypes.ShortType),
    BOOLEAN(Boolean.class, DataTypes.BooleanType);

    private Class javaType;
    private DataType sparkType;

    /**
     * Constructor for enum type. Take java type and his equivalent spark type.
     *
     * @param javaType  type of java object
     * @param sparkType type of spark object
     */
    SparkReversibleType(Class javaType, DataType sparkType) {
        this.javaType = javaType;
        this.sparkType = sparkType;
    }

    /**
     * Find {@link SparkReversibleType} based on spark {@link DataType}
     *
     * @param type {@link DataType} for search
     * @return {@link SparkReversibleType} what correspond to input {@link DataType}
     */
    public static SparkReversibleType reverseFromSpark(DataType type) {
        for (SparkReversibleType reversibleType : SparkReversibleType.values()) {

            if (type.sameType(reversibleType.getSparkType()))
                return reversibleType;
        }

        return null;
    }

    /**
     * Find {@link SparkReversibleType} based on java type
     *
     * @param type {@link Class} for search
     * @return {@link SparkReversibleType} what correspond to input {@link Class}
     */
    public static SparkReversibleType reverseFromJavaType(Class type) {
        for (SparkReversibleType reversibleType : SparkReversibleType.values()) {

            if (type.isAssignableFrom(reversibleType.javaType))
                return reversibleType;
        }

        return null;
    }

    /**
     * Return corresponding to enum java type
     *
     * @return {@link Class} corresponding to java type
     */
    public Class getJavaType() {
        return javaType;
    }

    /**
     * Return corresponding to enum spark type
     *
     * @return {@link DataType} correspondign to spark type
     */
    public DataType getSparkType() {
        return sparkType;
    }
}
