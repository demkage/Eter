package com.eter.spark.data.analysis.recommender;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import scala.Serializable;

/**
 * Created by rusifer on 4/2/17.
 */
public class ALSAnalysis implements Serializable {
    private static final long serialVersionUID = -6247257624775552814L;

    private Integer rank;
    private Integer numIterations;
    private Double lambda;
    private String userIdColumn;
    private String productIdColumn;
    private String ratingColumn;
    private MatrixFactorizationModel model;

    public ALSAnalysis() {
        rank = 10;
        numIterations = 10;
        lambda = 0.01;
        userIdColumn = "userId";
        productIdColumn = "productId";
        ratingColumn = "rating";
    }

    public Double predict(Integer userId, Integer productId) {
        if (model != null)
            return model.predict(userId, productId);
        else
            return Double.valueOf(0.0);
    }

    public Rating[] recommendProducts(Integer userId, Integer num) {
        if (model != null)
            return model.recommendProducts(userId, num);
        else
            return new Rating[]{null};
    }

    public void train(Dataset<Row> dataset) {
        model = ALS.train(transformToRDD(dataset).rdd(), rank, numIterations, lambda);
    }

    public JavaRDD<Rating> transformToRDD(Dataset<Row> dataset) {
        return dataset.javaRDD().map(new Function<Row, Rating>() {
            public Rating call(Row value) {
                int userIdIndex = value.fieldIndex(userIdColumn);
                int productIdIndex = value.fieldIndex(productIdColumn);
                int ratingIndex = value.fieldIndex(ratingColumn);
                return new Rating((int) value.getLong(userIdIndex),
                        (int) value.getLong(productIdIndex),
                        value.getDouble(ratingIndex));
            }
        }).cache();
    }
}
