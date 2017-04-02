package com.eter.spark.data.analysis.recommender;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Created by rusifer on 4/2/17.
 */
public class RatingAnalysis {
    private String userIdColumn;
    private String orderIdColumn;
    private String productIdColumn;
    private String categoryIdColumn;
    private String outputCustomerColumn;
    private String outputProductColumn;
    private String outputRatingColumn;

    public RatingAnalysis() {
        this.userIdColumn = "customerId";
        this.orderIdColumn = "orderId";
        this.productIdColumn = "productId";
        this.categoryIdColumn = "categoryId";
        this.outputCustomerColumn = "userId";
        this.outputProductColumn = "productId";
        this.outputRatingColumn = "rating";
    }

    public String getUserIdColumn() {
        return userIdColumn;
    }

    public void setUserIdColumn(String userIdColumn) {
        this.userIdColumn = userIdColumn;
    }

    public String getOrderIdColumn() {
        return orderIdColumn;
    }

    public void setOrderIdColumn(String orderIdColumn) {
        this.orderIdColumn = orderIdColumn;
    }

    public String getProductIdColumn() {
        return productIdColumn;
    }

    public void setProductIdColumn(String productIdColumn) {
        this.productIdColumn = productIdColumn;
    }

    public String getCategoryIdColumn() {
        return categoryIdColumn;
    }

    public void setCategoryIdColumn(String categoryIdColumn) {
        this.categoryIdColumn = categoryIdColumn;
    }

    public String getOutputCustomerColumn() {
        return outputCustomerColumn;
    }

    public void setOutputCustomerColumn(String outputCustomerColumn) {
        this.outputCustomerColumn = outputCustomerColumn;
    }

    public String getOutputProductColumn() {
        return outputProductColumn;
    }

    public void setOutputProductColumn(String outputProductColumn) {
        this.outputProductColumn = outputProductColumn;
    }

    public String getOutputRatingColumn() {
        return outputRatingColumn;
    }

    public void setOutputRatingColumn(String outputRatingColumn) {
        this.outputRatingColumn = outputRatingColumn;
    }

    public Dataset<Row> transfrom(Dataset<Row> dataset) {

        /*Dataset<Row> categoryResult = dataset.rollup(userIdColumn, categoryIdColumn)
                .count();
        categoryResult = categoryResult.where(categoryResult.col(categoryIdColumn).isNotNull());

        Dataset<Row> productResult = dataset.rollup(userIdColumn, productIdColumn)
                .count();
        productResult = productResult.where(productResult.col(productIdColumn).isNotNull());

        Dataset<Row> result = categoryResult.join(productResult,
                categoryResult.col(userIdColumn).equalTo(productResult.col(userIdColumn)));

        result = result.groupBy(categoryResult.col(userIdColumn),
                productResult.col(productIdColumn),
                categoryResult.col("count"),
                productResult.col("count"))
                .agg(categoryResult.col("count").multiply(0.3)
                        .plus(productResult.col("count").multiply(0.7))
                        .as(outputRatingColumn));

        */
        //dataset.sqlContext().cacheTable("table" + hashCode());
        String tableName = "table" + hashCode();
        tableName = tableName.replace("-", "");
        dataset.createOrReplaceTempView(tableName);
        Dataset<Row> result = dataset.sqlContext().sql(
                "SELECT catTemp." + userIdColumn + ", prTemp." + productIdColumn +
                        ", CAST(catTemp.categoryCount * 0.3 + prTemp.productCount * 0.7 AS double) AS " + outputRatingColumn +
                        "\nFROM ( SELECT " + userIdColumn + ", COUNT(*) AS categoryCount " +
                        "\nFROM " + tableName +
                        "\nGROUP BY " + userIdColumn + ", " + categoryIdColumn + ") catTemp" +
                        "\nJOIN (SELECT " + userIdColumn + ", " + productIdColumn + ", COUNT(*) AS productCount" +
                        "\nFROM " + tableName +
                        "\nGROUP BY " + userIdColumn + ", " + productIdColumn + ") prTemp " +
                        "\nON " + "catTemp." + userIdColumn + " = " + "prTemp." + userIdColumn);
        //"\nGROUP BY catTemp." + userIdColumn + ", prTemp." + productIdColumn);
        /*dataset.sqlContext().sql(
                "SELECT " + userIdColumn + ", " + categoryIdColumn + ", COUNT(*) AS categoryCount" +
                        "\nFROM " + tableName +
                        "\nGROUP BY " + userIdColumn + ", " + categoryIdColumn
        ).createOrReplaceTempView(tableName + "categoryCount");

        dataset.sqlContext().sql(
                "SELECT " + userIdColumn + ", " + productIdColumn + ", COUNT(*) AS productCount" +
                        "\nFROM " + tableName +
                        "\nGROUP BY " + userIdColumn + ", " + productIdColumn
        ).createOrReplaceTempView(tableName + "productCount");*/


        dataset.sqlContext().dropTempTable(tableName);
        //return dataset;
        return result.select(result.col(userIdColumn).as(outputCustomerColumn),
                result.col(productIdColumn).as(outputProductColumn),
                result.col(outputRatingColumn));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RatingAnalysis that = (RatingAnalysis) o;

        if (getUserIdColumn() != null ? !getUserIdColumn().equals(that.getUserIdColumn()) : that.getUserIdColumn() != null)
            return false;
        if (getOrderIdColumn() != null ? !getOrderIdColumn().equals(that.getOrderIdColumn()) : that.getOrderIdColumn() != null)
            return false;
        if (getProductIdColumn() != null ? !getProductIdColumn().equals(that.getProductIdColumn()) : that.getProductIdColumn() != null)
            return false;
        if (getCategoryIdColumn() != null ? !getCategoryIdColumn().equals(that.getCategoryIdColumn()) : that.getCategoryIdColumn() != null)
            return false;
        if (getOutputCustomerColumn() != null ? !getOutputCustomerColumn().equals(that.getOutputCustomerColumn()) : that.getOutputCustomerColumn() != null)
            return false;
        if (getOutputProductColumn() != null ? !getOutputProductColumn().equals(that.getOutputProductColumn()) : that.getOutputProductColumn() != null)
            return false;
        return getOutputRatingColumn() != null ? getOutputRatingColumn().equals(that.getOutputRatingColumn()) : that.getOutputRatingColumn() == null;
    }

    @Override
    public int hashCode() {
        int result = getUserIdColumn() != null ? getUserIdColumn().hashCode() : 0;
        result = 31 * result + (getOrderIdColumn() != null ? getOrderIdColumn().hashCode() : 0);
        result = 31 * result + (getProductIdColumn() != null ? getProductIdColumn().hashCode() : 0);
        result = 31 * result + (getCategoryIdColumn() != null ? getCategoryIdColumn().hashCode() : 0);
        result = 31 * result + (getOutputCustomerColumn() != null ? getOutputCustomerColumn().hashCode() : 0);
        result = 31 * result + (getOutputProductColumn() != null ? getOutputProductColumn().hashCode() : 0);
        result = 31 * result + (getOutputRatingColumn() != null ? getOutputRatingColumn().hashCode() : 0);
        return result;
    }
}
