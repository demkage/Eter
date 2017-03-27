package com.eter.spark.data.database.impl.spark;

import com.eter.spark.data.database.DatabaseProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Describe spark sql session properties.
 */
public class SparkSQLProperties implements DatabaseProperties {
    private Map<String, String> properties;
    private String configFile;

    public SparkSQLProperties() {
        properties = new HashMap<>();
    }

    /**
     * Put database property as key-value.
     *
     * @param key   property key
     * @param value property value
     */
    @Override
    public void put(String key, String value) {
        properties.put(key, value);
    }

    /**
     * Get database property value.
     *
     * @param key property key
     * @return property value
     */
    @Override
    public String get(String key) {
        return properties.get(key);
    }

    /**
     * Return whole map with all stored properties.
     *
     * @return map with stored properties
     */
    @Override
    public Map<String, String> getAsMap() {
        return properties;
    }

    /**
     * Using config file from where to read properties.
     *
     * @param path path to file.
     */
    @Override
    public void applyConfigFile(String path) {
        this.configFile = path;

    }

    /**
     * Return native spark sql properties.
     *
     * @return native spark sql properties
     */
    public Properties getAsSupportedProprietes() {
        Properties sqlProperties = new Properties();
        sqlProperties.putAll(properties);
        return sqlProperties;
    }
}
