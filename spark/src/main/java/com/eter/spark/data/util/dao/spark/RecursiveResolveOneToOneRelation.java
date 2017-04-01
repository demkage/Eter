package com.eter.spark.data.util.dao.spark;

import com.eter.spark.data.dao.impl.spark.SparkSQLDAO;
import com.eter.spark.data.util.transform.reflect.MethodSolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;

import javax.persistence.JoinColumn;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;

/**
 * Useful class to resolve sql relation such as One-To-One
 */
public class RecursiveResolveOneToOneRelation {
    private SparkSQLDAO sparkSQLDAO;

    /**
     * Default constructor
     */
    public RecursiveResolveOneToOneRelation() {
    }

    /**
     * Constructor what take as argument object of type {@link SparkSQLDAO}
     *
     * @param sparkSQLDAO DAO instance
     * @see SparkSQLDAO
     */
    public RecursiveResolveOneToOneRelation(SparkSQLDAO sparkSQLDAO) {
        this.sparkSQLDAO = sparkSQLDAO;
    }

    /**
     * Get current {@link SparkSQLDAO}
     *
     * @return DAO instance, or null if not exist
     */
    public SparkSQLDAO getSparkSQLDAO() {
        return sparkSQLDAO;
    }

    /**
     * Set DAO instance of type {@link SparkSQLDAO}
     *
     * @param sparkSQLDAO DAO instance
     */
    public void setSparkSQLDAO(SparkSQLDAO sparkSQLDAO) {
        this.sparkSQLDAO = sparkSQLDAO;
    }

    /**
     * Try to recursive solve joins based on annotation {@link JoinColumn}
     *
     * @param type {@link Class} what represent clss type
     * @param <T>  generic type of entity
     * @return {@link Dataset} with generic type {@link Row} as result of join
     */
    public <T> Dataset<Row> resolveJoins(Class<T> type) {
        Collection<Method> relationalMethods = MethodSolver.getRelationMethods(type);
        Dataset<Row> currentRows = null;

        if (relationalMethods.size() != 0) {
            currentRows = sparkSQLDAO.getAllAsDataset(type);
            Iterator<Method> iterator = relationalMethods.iterator();

            while (iterator.hasNext()) {
                Method method = iterator.next();
                currentRows = solveJoinIteration(method, currentRows);
            }

        } else {
            return sparkSQLDAO.getAllAsDataset(type);
        }

        return currentRows;

    }

    /**
     * Resolve one-to-one relation.
     *
     * @param type {@link Class} of entity to solve relation
     * @param <T>  generic type of entity
     * @return {@link Dataset<T>} with all objects and all their reference
     */
    public <T> Dataset<T> resolveRelation(Class<T> type) {
        Dataset<Row> joinResult = resolveJoins(type);
        Encoder encoder = Encoders.bean(type);

        return joinResult.map(new RecursiveJoinRowToJavaObjectMap<T>(type),
                encoder);
    }

    /**
     * Solve current join relation.
     *
     * @param method     {@link Method} what has {@link JoinColumn} annotation
     * @param rowDataset current {@link Dataset}
     * @return current {@link Dataset} joined with indicated relation
     */
    private Dataset<Row> solveJoinIteration(Method method, Dataset<Row> rowDataset) {
        Class referencedType = method.getReturnType();
        JoinColumn joinColumn = method.getDeclaredAnnotation(JoinColumn.class);

        rowDataset = JoinDatasetUtil.joinDatasets(rowDataset, resolveJoins(referencedType),
                joinColumn.name(), joinColumn.referencedColumnName(),
                referencedType.getSimpleName().toLowerCase());

        return rowDataset;
    }
}
