package minsait.ttaa.datio.engine.base;

import minsait.ttaa.datio.engine.Writer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.jetbrains.annotations.NotNull;

import static minsait.ttaa.datio.common.Common.*;

public abstract class BaseTransformer implements Writer {
    @NotNull
    private final SparkSession spark;

    private final TypeOutput typeOutput;

    protected Dataset<Row> dataframe;

    protected BaseTransformer(@NotNull SparkSession spark, @NotNull Dataset<Row> dataframe) {
        this.spark = spark;
        this.typeOutput = TypeOutput.getOption(TYPE_OUTPUT);
        this.dataframe = dataframe;
    }

    public void execute() {
        clean();
        extract();
        filter();
        select();
        retrieve();
    }

    public Dataset<Row> getDataframe() {
        return dataframe;
    }

    /*
     *Apply filters to data
     * */
    protected abstract void clean();

    /*
     * Apply functions to data
     * */
    protected abstract void extract();

    /*
     * Apply filters to data
     * */
    protected abstract void filter();

    /*
     * Select fields to data
     * */
    protected abstract void select();

    /*
     * Print depends on type
     * CONSOLE: print by console
     * FILE: print output to file
     * */
    protected void retrieve() {
        switch (typeOutput) {
            case CONSOLE:
                dataframe.show(NUM_FILAS_RETRIEVE, false);
                break;
            case FILE:
                write();
                break;
        }
    }


}
