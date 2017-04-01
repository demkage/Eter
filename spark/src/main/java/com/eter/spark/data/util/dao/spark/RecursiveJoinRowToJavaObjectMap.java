package com.eter.spark.data.util.dao.spark;

import com.eter.spark.data.util.transform.reflect.MethodSolver;
import com.eter.spark.data.util.transform.reflect.ReflectRowToObject;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Row;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.List;

/**
 * Recursive useful class for solve one to one relation
 */
public class RecursiveJoinRowToJavaObjectMap<T> implements MapFunction<Row, T> {
    private static final long serialVersionUID = -4321970853050042589L;

    private Class<T> referenceType;

    /**
     * Constructor what take {@link Class} as argument.
     *
     * @param referenceType type of reference object
     */
    public RecursiveJoinRowToJavaObjectMap(Class<T> referenceType) {
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

        return (T) solveRelations(value, referenceType, "");
    }

    /**
     * Recursive solve one-to-one relation/
     *
     * @param value            current {@link Row} value
     * @param recReferenceType recursive {@link Class} object type
     * @param alias            recursive solution alias
     * @return reflected object value
     * @throws InstantiationException    when can't create instance of object
     * @throws NoSuchMethodException     when can't find setter method
     * @throws IllegalAccessException    when is incorrect access method
     * @throws InvocationTargetException when is not an implicit public constructor of object
     */
    private Object solveRelations(Row value, Class recReferenceType, String alias) throws InstantiationException, NoSuchMethodException,
            IllegalAccessException, InvocationTargetException {

        Object baseObject = ReflectRowToObject.reflect(value, recReferenceType);
        List<Method> joinMethods = MethodSolver.getRelationMethods(recReferenceType);

        for (Method method : joinMethods) {
            Class referencedType = method.getReturnType();
            Object referencedObject = referencedType.newInstance();
            Collection<Method> referencedJoinMethods = MethodSolver.getRelationMethods(referencedType);

            if (referencedJoinMethods.size() != 0) {
                referencedObject = solveRelations(value, referencedType,
                        alias + referencedType.getSimpleName().toLowerCase());
            }

            referencedObject = solveReferencedFields(value, referencedObject, referencedType,
                    alias + referencedType.getSimpleName().toLowerCase());

            String referenceFieldName = method.getName().replace("get", "").toLowerCase();
            Method referenceSetter = MethodSolver.getSetterMethod(referenceFieldName, recReferenceType);

            referenceSetter.invoke(baseObject, referencedObject);
        }
        return baseObject;
    }

    /**
     * Set values for current object from {@link Row} value.
     *
     * @param value            current {@link Row} value
     * @param referencedObject {@link Object} to set values.
     * @param referencedType   {@link Class} of referenced {@link Object}
     * @param alias            current alias for fields in {@link Row}
     * @return {@link Object} of type {@code referencedType} with values from {@code value}
     * @throws IllegalAccessException    when is incorrect access method
     * @throws InvocationTargetException when is not an implicit public constructor of object
     */
    private Object solveReferencedFields(Row value, Object referencedObject, Class referencedType, String alias)
            throws InvocationTargetException, IllegalAccessException {

        for (Field referencedField : referencedType.getDeclaredFields()) {
            String fieldName = referencedField.getName();
            Method setter = MethodSolver.getSetterMethod(fieldName, referencedType);
            if (!setter.getParameterTypes()[0].getName().startsWith("java.lang"))
                continue;

            int fieldIndex = value.fieldIndex(alias + fieldName.toLowerCase());

            Object fieldValue = value.get(fieldIndex);

            setter.invoke(referencedObject, fieldValue);
        }

        return referencedObject;
    }
}
