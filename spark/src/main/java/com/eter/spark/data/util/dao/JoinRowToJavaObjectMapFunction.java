package com.eter.spark.data.util.dao;

import com.eter.spark.data.util.transform.reflect.MethodSolver;
import com.eter.spark.data.util.transform.reflect.ReflectRowToObject;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Used for transform a {@link Row} in {@link org.apache.spark.sql.Dataset} to user define type,
 * and resolve one-to-one relation. (Join datasets)
 */
public class JoinRowToJavaObjectMapFunction<T> implements MapFunction<Row, T> {
    private static final long serialVersionUID = 7330426657743857399L;

    private Class<T> referenceType;

    /**
     * Constructor for creating new object of this type. Use argument for getting class information.
     *
     * @param referenceType class of object to transform
     */
    public JoinRowToJavaObjectMapFunction(Class<T> referenceType) {
        this.referenceType = referenceType;
    }

    /**
     * Is called from {@link org.apache.spark.sql.Dataset#map} function.
     *
     * @param value Row value to transform
     * @return result java object
     * @throws InstantiationException    when can't create instance of object
     * @throws NoSuchMethodException     when can't find setter method
     * @throws IllegalAccessException    when is incorrect access method
     * @throws InvocationTargetException when is not an implicit public constructor of object
     */
    @Override
    public T call(Row value) throws InstantiationException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException {

        T baseObject = ReflectRowToObject.reflect(value, referenceType);
        List<Method> joinMethods = MethodSolver.getRelationMethods(referenceType);

        for (Method method : joinMethods) {
            Class referencedType = method.getReturnType();
            Object referencedObject = referencedType.newInstance();

            for (Field referencedField : referencedType.getDeclaredFields()) {
                String fieldName = referencedField.getName();
                Method setter = MethodSolver.getSetterMethod(fieldName, referencedType);

                int fieldIndex = value.fieldIndex(referencedType.getSimpleName().toLowerCase()
                        + fieldName.toLowerCase());

                Object fieldValue = value.get(fieldIndex);

                setter.invoke(referencedObject, fieldValue);
            }

            String referenceFieldName = method.getName().replace("get", "").toLowerCase();
            Method referenceSetter = MethodSolver.getSetterMethod(referenceFieldName, referenceType);

            referenceSetter.invoke(baseObject, referencedObject);
        }

        return baseObject;
    }
}
