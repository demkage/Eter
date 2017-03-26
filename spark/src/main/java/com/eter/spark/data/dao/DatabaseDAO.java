package com.eter.spark.data.dao;

import com.eter.spark.data.database.Connection;

import java.util.Collection;
import java.util.List;

/**
 * Interface for Data Access Object.
 */
public interface DatabaseDAO {

    /**
     * Return database connection.
     *
     * @return database connection
     */
    Connection getDatabaseConnection();

    /**
     * Set database connection.
     *
     * @param databaseConnection database connection
     */
    void setDatabaseConnection(Connection databaseConnection);

    /**
     * Save object to database.
     *
     * @param o   object to save
     * @param <T> type of object to save
     */
    <T> void save(final T o);

    /**
     * Delete object from database.
     *
     * @param o reference to object for delete.
     */
    void delete(final Object o);

    /**
     * Return object of concrete type with concrete id.
     *
     * @param type {@link Class} what will indicate object type
     * @param id   unique identifier of object
     * @param <T>  generic type of object
     * @return object with concrete type and concrete id
     */
    <T> T get(final Class<T> type, final Long id);

    /**
     * Save or update current object to database.
     *
     * @param o   object to save or update
     * @param <T> generic type of object
     */
    <T> void saveOrUpdate(final T o);

    /**
     * Save all objects from collection.
     *
     * @param collection {@link Collection} with all objects to save
     * @param <T>        generic type of element in collection
     */
    <T> void saveAll(List<T> collection);

    /**
     * Return all objects of concrete type
     *
     * @param type {@link Class} what will indicate type to search
     * @param <T>  generic type of object to search
     * @return all objects of concrete type
     */
    <T> Collection<T> getAll(final Class<T> type);

    /**
     * Commit all made changes.
     */
    void commit();

    /**
     * Discard all made changes.
     */
    void rollback();
}
