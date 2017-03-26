package com.eter.spark.data.util.dao;

import com.eter.spark.data.dao.impl.spark.SparkSQLDAO;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.Serializable;

import javax.persistence.JoinColumn;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * Created by rusifer on 3/26/17.
 */
public class SparkSQLRelationResolver<T, U> implements Serializable {
    private static final long serialVersionUID = -6055546003055747971L;

    /*public static <T> Encoder<T> resolve(Class<T> type, Encoder<T> encoder) {
        for (Field field : type.getFields()) {
            if (field.isAnnotationPresent(JoinColumn.class)) {
                String columnName = type.getAnnotation(JoinColumn.class).name();
                String cleanName = getCleanName(columnName);
                encoder.schema().drop()

            }
        }
    }

    private static String getCleanName(String name) {
        return name.replace("get", "").toLowerCase();
    }*/

    private static String getColumnNameFromMethod(Method method) {
        return method.getName().replace("get", "").toLowerCase();
    }

    public <V> U getOneToOneReferencedObject(List<U> objects, String referenceColumn, V referenceValue, Class<U> referencedType) {
        Optional<Method> getter = Optional.ofNullable(MethodSolver.getGetterMethod(referenceColumn, referencedType));
        final ResultHolder<U> result = new ResultHolder<>();
        getter.ifPresent((method) -> {
            Iterator<U> iterator = objects.iterator();
            while (iterator.hasNext()) {
                try {
                    U object = iterator.next();
                    Object returnValue = method.invoke(object, null);
                    if (referenceValue.equals(returnValue)) {
                        result.setResultValue(object);
                        break;
                    }

                } catch (IllegalAccessException | InvocationTargetException e) {
                    //TODO: something
                }
            }
        });

        return result.getResultValue();

    }

    public Dataset<Row> getReferencedDataset(SparkSQLDAO dao, Class<U> referencedType) {
        return dao.getAllAsDataset(referencedType);
    }

    public Dataset<Row> joinDatasets(Dataset<Row> firstDataset, Dataset<Row> secondDataset,
                                     String joinColumn, String referenceColumn, String alias) {
        Dataset<Row> secondDatasetRenamed = secondDataset;
        for (String column : secondDataset.columns()) {
            secondDatasetRenamed = secondDatasetRenamed.withColumnRenamed(column, alias + column);
        }

        return firstDataset.join(secondDatasetRenamed, firstDataset.col(joinColumn).equalTo(secondDatasetRenamed.col(alias + referenceColumn)));
    }

    public Dataset<Row> joinRelations(SparkSQLDAO dao, Dataset<Row> rows, Class<T> referenceClass, List<Method> relationMethods) {

        for (Method method : relationMethods) {
            JoinColumn joinColumnAnt = method.getDeclaredAnnotation(JoinColumn.class);
            String joinColumn = joinColumnAnt.name();
            String referencedColumn = joinColumnAnt.referencedColumnName();
            Class referencedType = method.getReturnType();
            Dataset<Row> secondRows = dao.getAllAsDataset(method.getReturnType());
            rows = joinDatasets(rows, secondRows,
                    joinColumn, referencedColumn, referencedType.getSimpleName().replace("get", "").toLowerCase());
        }

        return rows;
    }

    public Dataset<T> resolveRelation(Dataset<Row> rows, Class<T> referenceClass) {
        return rows.map(new JoinRowToJavaObjectMapFunction<T>(referenceClass),
                Encoders.bean(referenceClass));
    }

    class ResultHolder<R> {
        private R resultValue;

        public R getResultValue() {
            return resultValue;
        }

        public void setResultValue(R resultValue) {
            this.resultValue = resultValue;
        }
    }
}
