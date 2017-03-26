package com.eter.spark.data.database.impl.spark;

import com.eter.spark.data.database.Connection;
import com.eter.spark.data.database.DatabaseProperties;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Specific for Spark-sql package {@link Connection} implementation.
 */
public class SparkSQLConnection implements Connection {
    DatabaseProperties databaseProperties;
    SparkSession sparkSession;

    /**
     * Create {@link SparkSession} what can be used for realize JDBC connection.
     *
     * @return true if {@link SparkSession} is created, false otherwise
     */
    @Override
    public boolean connect() {
        if (sparkSession == null && databaseProperties != null) {
            sparkSession = SparkSession
                    .builder()
                    .appName(databaseProperties.get("appName"))
                    .config("spark.sql.warehouse.dir", databaseProperties.get("warehouse-dir"))
                    .master(databaseProperties.get("master"))
                    //TODO: other properties
                    .getOrCreate();

        }

        return sparkSession != null;

    }

    /**
     * Close created {@link SparkSession}.
     */
    @Override
    public void close() {
        if (sparkSession != null)
            sparkSession.close();
    }

    /**
     * Apply properties for realize connection.
     *
     * @param properties database connection properties
     */
    @Override
    public void applyProperties(DatabaseProperties properties) {
        this.databaseProperties = properties;
    }

    /**
     * Get current database properties.
     *
     * @return database properties.
     */
    @Override
    public DatabaseProperties getProperties() {
        return databaseProperties;
    }

    /**
     * Return created spark sql session.
     *
     * @return spark sql session
     */
    public SparkSession getSparkSession() {
        return sparkSession;
    }

    public Properties getAsSupportProperties() {
        if (databaseProperties instanceof SparkSQLProperties)
            return ((SparkSQLProperties) databaseProperties).getAsSupportedProprietes();
        return null;
    }
}
