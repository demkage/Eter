package com.eter.spark.data.util.dao.spark;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by rusifer on 4/1/17.
 */
public class JoinStrategy {
    private Map<Class, JoinSelector> joinMap;

    public JoinStrategy() {
        joinMap = new HashMap<>();
    }

    public Map<Class, JoinSelector> getJoinMap() {
        return joinMap;
    }

    public void setJoinMap(Map<Class, JoinSelector> joinMap) {
        this.joinMap = joinMap;
    }

    public void addJoinSelector(Class forType, JoinSelector selector) {
        joinMap.put(forType, selector);
    }

    public JoinSelector getJoinSelector(Class forType) {
        return joinMap.get(forType);
    }
}
