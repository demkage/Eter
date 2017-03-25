package com.eter.spark.data.util.transform.reflect;

import com.eter.spark.data.entity.*;
import org.apache.spark.sql.api.java.StructField;
import org.apache.spark.sql.api.java.StructType;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Test for {@link EntityReflection}.
 * Check for all current entity and can work without addition modification
 * also if structure of entity is changed.
 */
public class EntityReflectionTest {
    private List<String> customerFields;
    private List<String> customerDetailFields;
    private List<String> orderFields;
    private List<String> orderDetailFields;
    private List<String> categoryFields;
    private List<String> productFields;

    private StructType customerSchema;
    private StructType customerDetailSchema;
    private StructType orderSchema;
    private StructType orderDetailSchema;
    private StructType categorySchema;
    private StructType productSchema;


    @Before
    public void setUp() {
        customerFields = reflectFieldsToString(Customer.class);
        customerDetailFields = reflectFieldsToString(CustomerDetail.class);
        orderFields = reflectFieldsToString(Order.class);
        orderDetailFields = reflectFieldsToString(OrderDetail.class);
        categoryFields = reflectFieldsToString(Category.class);
        productFields = reflectFieldsToString(Product.class);

        customerSchema = EntityReflection.reflectEntityToSparkSchema(Customer.class);
        customerDetailSchema = EntityReflection.reflectEntityToSparkSchema(CustomerDetail.class);
        orderSchema = EntityReflection.reflectEntityToSparkSchema(Order.class);
        orderDetailSchema = EntityReflection.reflectEntityToSparkSchema(OrderDetail.class);
        categorySchema = EntityReflection.reflectEntityToSparkSchema(Category.class);
        productSchema = EntityReflection.reflectEntityToSparkSchema(Product.class);

    }


    @Test
    public void reflectEntityToSparkSchema() throws Exception {

        assert (checkSchema(customerSchema, customerFields));
        assert (checkSchema(customerDetailSchema, customerDetailFields));
        assert (checkSchema(orderSchema, orderFields));
        assert (checkSchema(orderDetailSchema, orderDetailFields));
        assert (checkSchema(categorySchema, categoryFields));
        assert (checkSchema(productSchema, productFields));

    }

    private boolean checkSchema(StructType schema, List<String> reflectedFields) {
        for (StructField field : schema.getFields()) {
            if (!reflectedFields.contains(field.getName()))
                return false;
        }

        return true;
    }

    private <T> List<String> reflectFieldsToString(Class<T> tClass) {
        List<String> reflectedFields = new ArrayList<String>();
        for (Field field : tClass.getDeclaredFields()) {
            Class fieldType = field.getType();
            boolean isJavaType = fieldType.getName().startsWith("java.lang");
            if (isJavaType || fieldType.isEnum()) {
                reflectedFields.add(field.getName());
            } else if (!fieldType.isPrimitive()) {
                reflectedFields.add(field.getName() + "Id");
            }

        }

        return reflectedFields;
    }


}