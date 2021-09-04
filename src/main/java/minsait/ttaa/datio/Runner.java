package minsait.ttaa.datio;

import minsait.ttaa.datio.engine.base.BaseTransformer;
import minsait.ttaa.datio.usecases.TransformerExercise;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static minsait.ttaa.datio.common.Common.*;
import static minsait.ttaa.datio.common.Common.INPUT_PATH;

/*
 *@author Jorge Cadillo
 * La lectura de properties se encuentra en Common.java
 * El archivo de configuración se llama config.properties
 * El archivo con el desarrollo del ejercicio está en TransformerExercise.java
 * */
public class Runner {

    static SparkSession spark = SparkSession
            .builder()
            .master(SPARK_MODE)
            .getOrCreate();

    public static void main(String[] args) {
        BaseTransformer t = new TransformerExercise(spark, loadInput(spark, INPUT_PATH));
        t.execute();
    }

    public static Dataset<Row> loadInput(SparkSession sparkSession, String file) {
        return sparkSession.read()
                .option(HEADER, true)
                .option(INFER_SCHEMA, true)
                .csv(file);
    }

}
