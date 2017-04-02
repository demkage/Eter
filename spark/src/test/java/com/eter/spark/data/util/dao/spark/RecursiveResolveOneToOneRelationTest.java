package com.eter.spark.data.util.dao.spark;

import com.eter.spark.data.dao.impl.spark.SparkSQLDAO;
import com.eter.spark.data.database.DatabaseProperties;
import com.eter.spark.data.database.impl.spark.SparkSQLConnection;
import com.eter.spark.data.database.impl.spark.SparkSQLProperties;
import com.eter.spark.data.entity.*;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Created by rusifer on 4/1/17.
 */
public class RecursiveResolveOneToOneRelationTest {
    static SparkSQLConnection connection;
    static SparkSQLDAO dao;

    @BeforeClass
    public static void setUp() throws Exception {
        connection = new SparkSQLConnection();
        DatabaseProperties properties = new SparkSQLProperties();
        properties.put("url", "jdbc:postgresql://localhost:5432/testdata");
        properties.put("user", "rusifer");
        properties.put("password", "");
        properties.put("warehouse-dir", "spark-warehouse");
        properties.put("appName", "TEST");
        properties.put("master", "local");
        connection.applyProperties(properties);
        connection.connect();
        dao = new SparkSQLDAO();
        dao.setDatabaseConnection(connection);
    }

    @Test
    public void resolveJoins() throws Exception {
        JoinStrategy joinStrategy = new JoinStrategy();
        joinStrategy.addJoinSelector(Order.class,
                JoinSelectorBuilder.createBuilder(Order.class)
                        .select(Order.class, "id")
                        .select(Order.class, "customerid")
                        .select(Order.class, "orderdetailid")
                        .join(Customer.class)
                        .select(Customer.class, "id")
                        .join(OrderDetail.class)
                        .select(OrderDetail.class, "id")
                        .select(OrderDetail.class, "productid")
                        .build());

        joinStrategy.addJoinSelector(OrderDetail.class,
                JoinSelectorBuilder.createBuilder(OrderDetail.class)
                        .select(OrderDetail.class, "id")
                        .select(OrderDetail.class, "productid")
                        .join(Product.class)
                        .select(Product.class, "id")
                        .build());

        joinStrategy.addJoinSelector(Customer.class,
                JoinSelectorBuilder.createBuilder(Customer.class)
                        .select(Customer.class, "id")
                        .select(Customer.class, "customerdetailid")
                        .build());

        joinStrategy.addJoinSelector(Product.class,
                JoinSelectorBuilder.createBuilder(Product.class)
                        .select(Product.class, "id")
                        .select(Product.class, "categoryid")
                        .join(Category.class)
                        .build());

        joinStrategy.addJoinSelector(Category.class,
                JoinSelectorBuilder.createBuilder(Category.class)
                        .select(Category.class, "id")
                        .build());

        RecursiveResolveOneToOneRelation solver = new RecursiveResolveOneToOneRelation(joinStrategy, dao);
        solver.resolveRelation(Order.class).foreach((order) -> System.out.println(order));
    }

}