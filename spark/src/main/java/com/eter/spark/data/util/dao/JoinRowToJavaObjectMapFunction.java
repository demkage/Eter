package com.eter.spark.data.util.dao;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Created by rusifer on 3/26/17.
 */
public class JoinRowToJavaObjectMapFunction<T> implements MapFunction<Row, T> {
    private static final long serialVersionUID = 7330426657743857399L;

    private Class<T> referenceType;

    public JoinRowToJavaObjectMapFunction(Class<T> referenceType) {
        this.referenceType = referenceType;
    }

    @Override
    public T call(Row value) throws Exception {
        T baseObject = ReflectRowToObject.reflect(value, referenceType);
        List<Method> joinMethods = MethodSolver.getRelationMethods(referenceType);

        for (Method method : joinMethods) {
            Class referencedType = method.getReturnType();
            Object referencedObject = referencedType.newInstance();
            for (Field referencedField : referencedType.getDeclaredFields()) {
                String fieldName = referencedField.getName();
                Method setter = MethodSolver.getSetterMethod(fieldName, referencedType);
                int fieldIndex = value.fieldIndex(referencedType.getSimpleName().toLowerCase() + fieldName.toLowerCase());
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
