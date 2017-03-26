package com.eter.spark.data.util.transform.reflect;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;

/**
 * Created by rusifer on 3/26/17.
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

    SparkReversibleType(Class javaType, DataType sparkType) {
        this.javaType = javaType;
        this.sparkType = sparkType;
    }

    public static SparkReversibleType reverseFromSpark(DataType type) {
        for (SparkReversibleType reversibleType : SparkReversibleType.values()) {
            if (type.sameType(reversibleType.getSparkType()))
                return reversibleType;
        }

        return null;
    }

    public static SparkReversibleType reverseFromJavaType(Class type) {
        for (SparkReversibleType reversibleType : SparkReversibleType.values()) {
            if (type.isAssignableFrom(reversibleType.javaType))
                return reversibleType;
        }

        return null;
    }

    public Class getJavaType() {
        return javaType;
    }

    public DataType getSparkType() {
        return sparkType;
    }
}
