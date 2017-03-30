package com.eter.spark.data.dao.impl.spark;

import com.eter.spark.data.dao.DatabaseDAO;
import com.eter.spark.data.dao.DatasetBasedDAO;
import com.eter.spark.data.database.Connection;
import com.eter.spark.data.database.impl.spark.SparkSQLConnection;
import com.eter.spark.data.util.dao.RowToJavaObjectMapFunction;
import com.eter.spark.data.util.dao.SparkSQLRelationResolver;
import org.apache.spark.sql.*;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import javax.persistence.Table;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by rusifer on 3/26/17.
 */
public class SparkSQLDAO implements DatabaseDAO, DatasetBasedDAO {
    private SparkSQLConnection sparkConnection;
    private List<DataFrameWriter> transactions;
    private SparkSQLRelationResolver relationResolver;
    /**
     * Return database connection.
     *
     * @return database connection
     */
    @Override
    public Connection getDatabaseConnection() {
        return sparkConnection;
    }

    /**
     * Set database connection.
     *
     * @param databaseConnection database connection
     */
    @Override
    public void setDatabaseConnection(Connection databaseConnection) {
        if (databaseConnection instanceof SparkSQLConnection)
            sparkConnection = (SparkSQLConnection) databaseConnection;
        else
            //TODO: throw exception of incorrect database connection
            return;
    }

    /**
     * Save object to database.
     * Not very well working and not recommended to use.
     *
     * @param o object to save
     */
    @Override
    public <T> void save(T o) {
        SparkSession session = sparkConnection.getSparkSession();
        if (session != null) {
            Encoder<T> objectEncoder = Encoders.bean((Class<T>) o.getClass());
            List<T> object = new ArrayList<T>();
            object.add(o);

            objectEncoder.schema().printTreeString();

            DataFrameWriter writer = session.createDataset(object, objectEncoder)
                    .write()
                    .format("jdbc").options(sparkConnection.getProperties().getAsMap());

            transactions.add(writer);

        }
    }

    /**
     * Delete object from database.
     * Not implemented.
     *
     * @param o reference to object for delete.
     */
    @Override
    public void delete(Object o) {
        throw new NotImplementedException();
    }

    /**
     * Return object of concrete type with concrete id.
     *
     * @param type {@link Class} what will indicate object type
     * @param id   unique identifier of object
     * @return object with concrete type and concrete id
     */
    @Override
    public <T> T get(Class<T> type, Long id) {
        SparkSession session = sparkConnection.getSparkSession();
        if (session != null) {
            Encoder<T> objectEncoder = Encoders.bean(type);
            Dataset<Row> rows = session.read()
                    .format("jdbc")
                    .options(sparkConnection.getProperties().getAsMap())
                    .load();
            Dataset<T> objects = rows.as(objectEncoder);
            return objects.first();
        }

        return null;
    }

    /**
     * Save or update current object to database.
     *
     * @param o object to save or update
     */
    @Override
    public <T> void saveOrUpdate(T o) {
        save(o);
    }

    /**
     * Save all objects from collection.
     *
     * @param collection {@link Collection} with all objects to save
     */
    @Override
    public <T> void saveAll(List<T> collection) {
        if (collection.size() == 0)
            return;

        SparkSession session = sparkConnection.getSparkSession();
        if (session != null) {
            Iterator<T> iterator = collection.iterator();
            Encoder<T> encoder = Encoders.bean((Class<T>) iterator.next().getClass());
            DataFrameWriter writer = session.createDataset(collection, encoder)
                    .write()
                    .format("jdbc")
                    .options(sparkConnection.getProperties().getAsMap());
            transactions.add(writer);

        }
    }

    /**
     * Return all objects of concrete type
     *
     * @param type {@link Class} what will indicate type to search
     * @return all objects of concrete type
     */
    @Override
    public <T> Collection<T> getAll(Class<T> type) {
        SparkSession session = sparkConnection.getSparkSession();
        if (session != null) {
            Encoder<T> objectEncoder = Encoders.bean(type);
            String tableName = type.getName().toLowerCase();

            if (type.isAnnotationPresent(Table.class)) {
                tableName = type.getAnnotation(Table.class).name();
            }


            Dataset<Row> rows = session.read()
                    .format("jdbc")
                    .option("dbtable", tableName)
                    .options(sparkConnection.getProperties().getAsMap())
                    .load();
            Dataset<T> objects = SparkSQLRelationResolver.resolveOneToOne(this, rows, type);
            return objects.collectAsList();
        }
        return null;
    }

    /**
     * Return data from database, in dependence of indicated {@link Class}, as {@link Dataset}.
     *
     * @param type {@link Class} of data to return
     * @param <T>  generic type of data to return
     * @return {@link Dataset} with selected data from database
     */
    @Override
    public <T> Dataset<Row> getAllAsDataset(Class<T> type) {
        SparkSession session = sparkConnection.getSparkSession();
        Dataset<Row> rows = null;
        if (session != null) {
            Encoder<T> objectEncoder = Encoders.bean(type);
            String tableName = type.getName().toLowerCase();

            if (type.isAnnotationPresent(Table.class)) {
                tableName = type.getAnnotation(Table.class).name();
            }

            rows = session.read()
                    .format("jdbc")
                    .option("dbtable", tableName)
                    .options(sparkConnection.getProperties().getAsMap())
                    .load();
        }

        return rows;
    }

    /**
     * Commit all made changes.
     */
    @Override
    public void commit() {
        Iterator<DataFrameWriter> it = transactions.iterator();
        while (it.hasNext())
            it.next().save();
    }

    /**
     * Discard all made changes.
     */
    @Override
    public void rollback() {
        transactions.clear();
    }
}
