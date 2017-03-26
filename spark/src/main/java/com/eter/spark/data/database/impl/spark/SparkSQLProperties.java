package com.eter.spark.data.database.impl.spark;

import com.eter.spark.data.database.DatabaseProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by rusifer on 3/26/17.
 */
public class SparkSQLProperties implements DatabaseProperties {
    private Map<String, String> properties;
    private String configFile;

    public SparkSQLProperties() {
        properties = new HashMap<>();
    }

    @Override
    public void put(String key, String value) {
        properties.put(key, value);
    }

    @Override
    public String get(String key) {
        return properties.get(key);
    }

    @Override
    public Map<String, String> getAsMap() {
        return properties;
    }

    @Override
    public void applyConfigFile(String path) {
        this.configFile = path;

    }

    public Properties getAsSupportedProprietes() {
        Properties sqlProperties = new Properties();
        sqlProperties.putAll(properties);
        return sqlProperties;
    }
}
