package com.eter.spark.data.util.dao;

import com.eter.spark.data.dao.impl.spark.SparkSQLDAO;
import com.eter.spark.data.util.transform.reflect.MethodSolver;
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
 * Useful methods for resolve sql relations such as one-to-one.
 */
public class SparkSQLRelationResolver implements Serializable {
    private static final long serialVersionUID = -6055546003055747971L;


    /**
     * Used for get inner join of two {@link Dataset}'s, using concrete columns indicated by arguments.
     * Name of columns for second {@link Dataset} will be renamed using alias argument:
     * columnName = alias + referenceColumn. For example:
     * For table 'category', with alias 'category', column 'id' will have name 'categoryid'.
     *
     * @param firstDataset    right join {@link Dataset}
     * @param secondDataset   left join {@link Dataset}
     * @param joinColumn      name of right {@link Dataset} column on join
     * @param referenceColumn name of left {@link Dataset} column on join
     * @param alias           alias of left {@link Dataset}
     * @return result of: {@code firstDataset} join {@code secondDataset} on {@code joinColumn} = {@code refernceColumn}
     */
    public static Dataset<Row> joinDatasets(Dataset<Row> firstDataset, Dataset<Row> secondDataset,
                                            String joinColumn, String referenceColumn, String alias) {

        Dataset<Row> secondDatasetRenamed = secondDataset;
        for (String column : secondDataset.columns()) {
            secondDatasetRenamed = secondDatasetRenamed.withColumnRenamed(column, alias + column);
        }

        return firstDataset.join(secondDatasetRenamed, firstDataset.col(joinColumn)
                .equalTo(secondDatasetRenamed.col(alias + referenceColumn)));
    }

    /**
     * Realize join for all relation's method indicated by 3rd argument.
     * All relation methods should have {@link JoinColumn} reference.
     *
     * @param dao             DAO object used for getting referenced tables
     * @param rows            rows with what will be joined referenced tables
     * @param relationMethods list with all methods declared as relational
     * @return {@link Dataset} joined with all relations
     */
    public static Dataset<Row> joinRelations(SparkSQLDAO dao, Dataset<Row> rows, List<Method> relationMethods) {

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

    /**
     * Transform from {@link Row} type to indicated {@code T} type with resolving relations.
     *
     * @param rows           {@link Dataset} what represent current data (should already be joined)
     * @param referenceClass {@link Class} of base object
     * @param <T>            generic type of base object
     * @return {@link Dataset} with all objects
     */
    public static <T> Dataset<T> resolveRelation(Dataset<Row> rows, Class<T> referenceClass) {
        return rows.map(new JoinRowToJavaObjectMapFunction<T>(referenceClass),
                Encoders.bean(referenceClass));
    }

    /**
     * Resolve OneToOne relation for objects of type {@code <T>}.
     *
     * @param dao      DAO object used for getting referenced tables
     * @param rows     rows with all data
     * @param forClass {@link Class} of base object
     * @param <T>      generic type of base object
     * @return {@link Dataset} with all objects
     */
    public static <T> Dataset<T> resolveOneToOne(SparkSQLDAO dao, Dataset<Row> rows, Class<T> forClass) {
        return resolveRelation(joinRelations(dao, rows, MethodSolver.getRelationMethods(forClass)), forClass);

    }
}
