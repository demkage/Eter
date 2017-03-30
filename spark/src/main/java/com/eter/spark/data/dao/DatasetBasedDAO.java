package com.eter.spark.data.dao;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Interface for access a data from database using {@link Dataset} as return type
 */
public interface DatasetBasedDAO {

    /**
     * Return data from database, in dependence of indicated {@link Class}, as {@link Dataset}.
     *
     * @param type {@link Class} of data to return
     * @param <T>  generic type of data to return
     * @return {@link Dataset} with selected data from database
     */
    public <T> Dataset<Row> getAllAsDataset(Class<T> type);
}
