package minsait.ttaa.datio.usecases;

import minsait.ttaa.datio.engine.base.BaseTransformer;
import minsait.ttaa.datio.usecases.data.AgeRange;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.OUTPUT_PATH;
import static minsait.ttaa.datio.common.naming.PlayerInput.*;
import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.SaveMode.Overwrite;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.when;


/*
 * @author Jorge Cadillo
 * */
public class TransformerExercise extends BaseTransformer {

    public TransformerExercise(@NotNull SparkSession spark, @NotNull Dataset<Row> dataframe) {
        super(spark, dataframe);
    }

    @Override
    protected void extract() {
        addAgeRange();
        addRankNationalityPosition();
        addPotentialOverall();
    }

    @Override
    protected void filter() {
        addFilterAgeRangeAndRankNationalitPosition();
    }

    //Item 5 del Ejercicio
    public void addFilterAgeRangeAndRankNationalitPosition() {
        dataframe = dataframe.filter(
                rankByNationalityPosition.column().$less(3)
                        .or(ageRange.column().isin(AgeRange.B.getRange(), AgeRange.C.getRange())
                                .and(potentialVsOverall.column().$greater(1.15)))
                        .or(ageRange.column().equalTo(AgeRange.A.getRange())
                                .and(potentialVsOverall.column().$greater(1.25)))
                        .or(ageRange.column().equalTo(AgeRange.D.getRange())
                                .and(rankByNationalityPosition.column().$less(5.0)))
        );
    }

    //Item 2 del Ejercicio
    public void addAgeRange() {
        Column ruleAgeRange =
                when(age.column().$less(AgeRange.A.getLimit()), AgeRange.A.getRange())
                        .when(age.column().$less(AgeRange.B.getLimit()), AgeRange.B.getRange())
                        .when(age.column().$less(AgeRange.C.getLimit()), AgeRange.C.getRange())
                        .otherwise(AgeRange.D.getRange());
        dataframe = dataframe.withColumn(ageRange.getName(), ruleAgeRange);
    }

    //Item 3 del ejercicio
    public void addRankNationalityPosition() {
        WindowSpec w = Window
                .partitionBy(nationality.column(), teamPosition.column())
                .orderBy(overall.column().desc());
        Column rowNumber = row_number().over(w);
        dataframe = dataframe.withColumn(rankByNationalityPosition.getName(), rowNumber);
    }

    //Item 4 del ejercicio
    public void addPotentialOverall() {
        Column columnDivision = potential.column().divide(overall.column());
        dataframe = dataframe.withColumn(potentialVsOverall.getName(), columnDivision);
    }

    //Item 1 del ejercicio
    @Override
    protected void select() {
        dataframe = dataframe.select(
                shortName.column(),
                longName.column(),
                age.column(),
                ageRange.column(),
                heightCm.column(),
                weightKg.column(),
                nationality.column(),
                clubName.column(),
                overall.column(),
                potential.column(),
                potentialVsOverall.column(),
                teamPosition.column(),
                rankByNationalityPosition.column()
        );
    }

    //Item 6 del ejercicio
    @Override
    public void write() {
        dataframe
                .coalesce(2)
                .write()
                .partitionBy(nationality.getName())
                .mode(Overwrite)
                .parquet(OUTPUT_PATH);
    }

    @Override
    protected void clean() {
    }
}
