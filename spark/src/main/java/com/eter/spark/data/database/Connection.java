package com.eter.spark.data.database;

/**
 * Define an interface for create and initialize a database connection.
 */
public interface Connection {
    /**
     * Connect to database using applied properties
     * TODO: throw some exception if configuration is incorrect.
     *
     * @return true if connection is successful, and false otherwise
     */
    boolean connect();

    /**
     * Close database connection.
     * TODO: throw some exception if can't close connection.
     */
    void close();

    /**
     * Apply database connection properties.
     *
     * @param properties database connection properties
     */
    void applyProperties(DatabaseProperties properties);

    /**
     * Return stored properties.
     *
     * @return stored properties
     */

    DatabaseProperties getProperties();
}
