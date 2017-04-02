package com.eter.spark.data.util.dao.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Useful class for implement {@link Dataset} join
 */
public class JoinDatasetUtil {

    /**
     * Used for get inner join of two {@link Dataset}'s, using concrete columns indicated by arguments.
     * Name of columns for second {@link Dataset} will be renamed using alias argument:
     * columnName = alias + referenceColumn. For example:
     * For table 'category', with alias 'category', column 'id' will have name 'categoryid'.
     *
     * @param firstDataset    right join {@link Dataset}
     * @param secondDataset   left join {@link Dataset}
     * @param joinColumn      name of right {@link Dataset} column on join
     * @param referenceColumn name of left {@link Dataset} column on join
     * @param alias           alias of left {@link Dataset}
     * @return result of: {@code firstDataset} join {@code secondDataset} on {@code joinColumn} = {@code refernceColumn}
     */
    public static Dataset<Row> joinDatasets(Dataset<Row> firstDataset, Dataset<Row> secondDataset,
                                            String joinColumn, String referenceColumn, String alias) {

        Dataset<Row> secondDatasetRenamed = secondDataset;
        for (String column : secondDataset.columns()) {
            secondDatasetRenamed = secondDatasetRenamed.withColumnRenamed(column, alias + column);
        }

        return firstDataset.join(secondDatasetRenamed, firstDataset.col(joinColumn)
                .equalTo(secondDatasetRenamed.col(alias + referenceColumn)))
                .drop(secondDatasetRenamed.col(alias + referenceColumn));
    }
}
