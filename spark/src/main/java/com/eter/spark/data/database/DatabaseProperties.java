package com.eter.spark.data.database;

import java.util.Map;

/**
 * Interface for describing database connection properties as key - value.
 * For exemple: "user" - "db_user", "password" - "db-password",
 * "url" - "jdbc:postgresql:dbserver".
 */
public interface DatabaseProperties {

    /**
     * Put database property as key-value.
     *
     * @param key   property key
     * @param value property value
     */
    void put(String key, String value);

    /**
     * Get database property value.
     *
     * @param key property key
     * @return property value
     */
    String get(String key);

    /**
     * Return whole map with all stored properties.
     *
     * @return map with stored properties
     */
    Map<String, String> getAsMap();

    /**
     * Using config file from where to read properties.
     *
     * @param path path to file.
     */
    void applyConfigFile(String path);
}
