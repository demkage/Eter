package com.eter.spark.data.dao.impl.spark;

import com.eter.spark.data.database.DatabaseProperties;
import com.eter.spark.data.database.impl.spark.SparkSQLConnection;
import com.eter.spark.data.database.impl.spark.SparkSQLProperties;
import com.eter.spark.data.entity.*;
import com.eter.spark.data.util.transform.reflect.EntityReflection;
import com.eter.spark.data.util.transform.reflect.MethodSolver;
import com.eter.spark.data.util.dao.SparkSQLRelationResolver;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Testing DAO object based on Spark SQL implementation
 */
public class SparkSQLDAOTest {
    static SparkSQLConnection connection;
    static SparkSQLDAO dao;

    /**
     * Setting up PostgreSQL database
     *
     * @throws Exception
     */
    @Before
    public void setUp() throws Exception {
//        connection = new SparkSQLConnection();
//        DatabaseProperties properties = new SparkSQLProperties();
//        properties.put("url", "jdbc:postgresql://localhost:5432/testdata");
//        properties.put("user", "rusifer");
//        properties.put("password", "");
//        properties.put("warehouse-dir", "spark-warehouse");
//        properties.put("appName", "TEST");
//        properties.put("master", "local");
//        connection.applyProperties(properties);
//        connection.connect();
//        dao = new SparkSQLDAO();
//        dao.setDatabaseConnection(connection);
    }

    /**
     * Setting up Microsoft SQL Server
     *
     * @throws Exception
     */
    @BeforeClass
    public static void setUpSQLServer() throws Exception {

        connection = new SparkSQLConnection();
        DatabaseProperties properties = new SparkSQLProperties();
        properties.put("url", "jdbc:sqlserver://localhost:1433;databaseName=NC_testdata");
        properties.put("user", "onlyforreading");
        properties.put("password", "");
        properties.put("warehouse-dir", "spark-warehouse");
        properties.put("appName", "TEST");
        properties.put("master", "local");
        connection.applyProperties(properties);
        connection.connect();
        dao = new SparkSQLDAO();
        dao.setDatabaseConnection(connection);

    }

    /**
     * Close current sql connection
     *
     * @throws Exception
     */
    @AfterClass
    public static void tearDown() throws Exception {
        connection.close();
    }


    @Test
    public void testGetAllProducts() {
        assert (dao.getAll(Product.class).size() != 0);
    }

    @Test
    public void testGetAllOrders() {
        assert (dao.getAll(Order.class).size() != 0);
    }

    @Test
    public void testGetAllOrdersDetail() {
        assert (dao.getAll(OrderDetail.class).size() != 0);
    }

    @Test
    public void testGetAllCustomers() {
        assert (dao.getAll(Customer.class).size() != 0);
    }

    @Test
    public void testGetAllCustomersDetail() {
        assert (dao.getAll(CustomerDetail.class).size() != 0);
    }

    @Test
    public void testGetAllCategories() {
        assert (dao.getAll(Category.class).size() != 0);
    }

    @Test
    public void testRelationCustomerCustomerDetail() {
        Dataset<Row> customers = dao.getAllAsDataset(Customer.class);

        SparkSQLRelationResolver.resolveOneToOne(dao, customers, Customer.class)
                .foreach((objectCustomer) -> {
                    assert (objectCustomer.getCustomerDetail() != null);
                });
    }

    @Test
    public void testRelationProductCategory() {
        Dataset<Row> products = dao.getAllAsDataset(Product.class);

        SparkSQLRelationResolver.resolveOneToOne(dao, products, Product.class)
                .foreach((objectProduct) -> {
                    assert (objectProduct.getCategory() != null);
                });
    }

    @Test
    public void testRelationOrderOrderDetailCustomer() {
        Dataset<Row> orders = dao.getAllAsDataset(Order.class);

        SparkSQLRelationResolver.resolveOneToOne(dao, orders, Order.class)
                .foreach((objectOrder) -> {
                    assert (objectOrder.getCustomer() != null);
                    assert (objectOrder.getOrderDetail() != null);
                });
    }

    @Test
    public void testRelationOrderDetailCustomerProduct() {
        Dataset<Row> ordersDetails = dao.getAllAsDataset(OrderDetail.class);

        SparkSQLRelationResolver.resolveOneToOne(dao, ordersDetails, OrderDetail.class)
                .foreach((objectOrderDetail) -> {
                    assert (objectOrderDetail.getProduct() != null);
                });
    }

    /*@Test
    public void getAll() throws Exception {
        Dataset<Row> products = dao.getAllAsDataset(Product.class);

        SparkSQLRelationResolver.resolveOneToOne(dao, products, Product.class)
                .foreach((objectProduct) -> {
                    assert (objectProduct.getCategory() != null);
                });


        StructType productsSchema = EntityReflection.reflectEntityToSparkSchema(Product.class);
        StructType categorySchema = EntityReflection.reflectEntityToSparkSchema(Category.class);
        Dataset<Row> productsDataset = dao.getAndJoinAsDataset(Product.class);

        for (StructField field : productsSchema.fields()) {
            try {
                productsDataset.schema().fieldIndex(field.name());
            } catch (IllegalArgumentException e) {
                assert (false) : "Field '" + field.name() + "' doesn't exist";
            }
        }

        for (StructField field : categorySchema.fields()) {
            try {
                if (!field.name().equals("id"))
                    assert (productsDataset.schema().fieldIndex(field.name()) >= 0);
            } catch (IllegalArgumentException e) {
                assert (false) : "Field '" + field.name() + "' doesn't exist";
            }
        }
    }*/

}