package minsait.ttaa.datio.usecases;

import minsait.ttaa.datio.engine.base.BaseTransformer;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.OUTPUT_PATH;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.catHeightByPosition;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.functions.rank;
import static org.apache.spark.sql.functions.when;

public class TransformerExample extends BaseTransformer {

    protected TransformerExample(@NotNull SparkSession spark,@NotNull Dataset<Row> dataframe) {
        super(spark, dataframe);
    }

    /**
     * A Dataset with filter transformation applied
     * column team_position != null && column short_name != null && column overall != null
     */
    @Override
    protected void clean() {
        dataframe = dataframe.filter(
                teamPosition.column().isNotNull().and(
                        shortName.column().isNotNull()
                ).and(
                        overall.column().isNotNull()
                )
        );
    }

    /**
     * add to the Dataset the column "cat_height_by_position"
     * by each position value
     * cat A for if is in 20 players tallest
     * cat B for if is in 50 players tallest
     * cat C for the rest
     */
    @Override
    protected void extract() {
        WindowSpec w = Window
                .partitionBy(teamPosition.column())
                .orderBy(heightCm.column().desc());
        Column rank = rank().over(w);
        Column rule = when(rank.$less(10), "A")
                .when(rank.$less(50), "B")
                .otherwise("C");
        dataframe = dataframe.withColumn(catHeightByPosition.getName(), rule);
    }

    @Override
    protected void filter() {
    }

    @Override
    protected void select() {
        dataframe = dataframe.select(
                shortName.column(),
                overall.column(),
                heightCm.column(),
                teamPosition.column(),
                catHeightByPosition.column()
        );
    }

    @Override
    public void write() {
        dataframe
                .coalesce(2)
                .write()
                .partitionBy(teamPosition.getName())
                .mode(Overwrite)
                .parquet(OUTPUT_PATH);
    }
}
