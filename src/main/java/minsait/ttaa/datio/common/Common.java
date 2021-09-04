package minsait.ttaa.datio.common;

import java.util.ResourceBundle;

public class Common {

    public static final String SPARK_MODE = "local[*]";
    public static final String HEADER = "header";
    public static final String INFER_SCHEMA = "inferSchema";
    public static String INPUT_PATH;
    public static String OUTPUT_PATH;
    public static String INPUT_PATH_TEST;
    public static int TYPE_OUTPUT;
    public static int NUM_FILAS_RETRIEVE;


    static {
        ResourceBundle properties = ResourceBundle.getBundle("config");
        INPUT_PATH = properties.getString("input_path");
        INPUT_PATH_TEST = properties.getString("input_path_test");
        OUTPUT_PATH = properties.getString("output_path");
        TYPE_OUTPUT = Integer.parseInt(properties.getString("type_output"));
        NUM_FILAS_RETRIEVE = Integer.parseInt(properties.getString("num_filas"));
    }

}
