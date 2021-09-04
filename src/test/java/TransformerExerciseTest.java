import minsait.ttaa.datio.common.naming.PlayerInput;
import minsait.ttaa.datio.common.naming.PlayerOutput;
import minsait.ttaa.datio.usecases.TransformerExercise;
import minsait.ttaa.datio.usecases.data.AgeRange;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static minsait.ttaa.datio.common.naming.PlayerOutput.*;
import static org.apache.spark.sql.types.DataTypes.StringType;

public class TransformerExerciseTest {

    private TransformerExercise transformer;
    private SparkSession spark;

    @Before
    public void setUp() {
        spark = SparkSession.builder()
                .master("local[2]")
                .appName("test")
                .getOrCreate();
    }


    //region addFilterAgeRangeAndRankNationalityPosition


    /*
     * Se prueba la primera cláusula: rankByNationalityPosition < 3
     * Cada satisfy cumple la cláusula
     * */
    @Test
    public void test_with_RankNationality_Position_less_3() {
        List<String> lineas = Arrays.asList(
                "Jorge,A,4,1.3",
                "Martin,B,2,1.133",//satisfy
                "Paco,B,3,0.34",
                "Susy,C,1,1.3",//satisfy
                "Marina,A,5,2.3",
                "Raquel,B,2,1.01",//satisfy
                "Christian,B,1,1.6",//satisfy
                "Fernando,D,3,0.987",
                "Pedro,A,2,2.1"//satisfy
        );


        transformer = new TransformerExercise(spark, spark.createDataFrame(getListRow(lineas), getStructType()));
        transformer.addFilterAgeRangeAndRankNationalitPosition();
        Assert.assertEquals(5, transformer.getDataframe().filter(PlayerOutput.rankByNationalityPosition.column().$less("3")).count());
    }

    /*
     * Se prueba la segunda cláusula: ageRange sea B o C y potencialOverall>1.15
     * Cada satisfy cumple la cláusula
     * */
    @Test
    public void test_with_RankNationality_ageRangeInByCAndPotencialOverallGreater1P15() {
        List<String> lineas = Arrays.asList(
                "Jorge,A,4,1.3",
                "Martin,B,2,1.90",//satisfy
                "Paco,B,3,0.34",
                "Susy,C,1,1.3",//satisfy
                "Marina,A,5,2.3",
                "Raquel,C,2,1.01",
                "Christian,B,1,1.6",//satisfy
                "Fernando,D,3,0.987",
                "Pedro,C,2,2.1"//satisfy
        );
        transformer = new TransformerExercise(spark, spark.createDataFrame(getListRow(lineas), getStructType()));
        transformer.addFilterAgeRangeAndRankNationalitPosition();
        Assert.assertEquals(4, transformer.getDataframe()
                .filter(ageRange.column().isin(AgeRange.B.getRange(), AgeRange.C.getRange())
                        .and(potentialVsOverall.column().$greater(1.15))).count()
        );
    }


    /*
     * Se prueba la tercera cláusula: ageRange=A y potencialOverall>1.25
     * Cada satisfy cumple la cláusula
     * */
    @Test
    public void test_with_RankNationality_ageRangeEqualAPotencialOverallGreater1P25() {
        List<String> lineas = Arrays.asList(
                "Jorge,A,4,1.3",//satisfy
                "Martin,B,2,1.90",
                "Paco,A,3,0.34",
                "Susy,C,1,1.3",
                "Marina,A,5,2.3",//satisfy
                "Raquel,C,2,1.01",
                "Christian,B,1,1.6",
                "Fernando,A,3,0.987",
                "Pedro,C,2,2.1",
                "Sara,A,2,2.3"//satisfy
        );
        transformer = new TransformerExercise(spark, spark.createDataFrame(getListRow(lineas), getStructType()));
        transformer.addFilterAgeRangeAndRankNationalitPosition();
        Assert.assertEquals(3, transformer.getDataframe()
                .filter(ageRange.column().equalTo(AgeRange.A.getRange())
                        .and(potentialVsOverall.column().$greater(1.25))).count()
        );
    }

    /*
     * Se prueba la cuarta cláusula: ageRange=D y potencialOverall>5.0
     * Cada satisfy cumple la cláusula.
     * */
    @Test
    public void test_with_RankNationality_ageRangeEqualDPotencialOverallGreater5() {
        List<String> lineas = Arrays.asList(
                "Jorge,A,4,1.3",
                "Martin,B,2,1.90",
                "Paco,A,3,0.34",
                "Susy,D,1,1.3",//satisfy
                "Marina,A,5,2.3",
                "Raquel,D,8,5.01",
                "Christian,B,1,1.6",
                "Fernando,D,8,9.987",
                "Pedro,C,2,2.1",
                "Sara,D,6,8.3"
        );
        transformer = new TransformerExercise(spark, spark.createDataFrame(getListRow(lineas), getStructType()));
        transformer.addFilterAgeRangeAndRankNationalitPosition();
        Assert.assertEquals(1, transformer.getDataframe()
                .filter(ageRange.column().equalTo(AgeRange.D.getRange())
                        .and(rankByNationalityPosition.column().$less(5.0))).count()
        );
    }

    //endregion

    //region addAgeRange

    /**
     * Se prueba el método addAgeRange donde se les asigna una letra según un rango de edad
     */
    @Test
    public void test_add_range_age() {
        List<String> lineas = Arrays.asList(
                "Jorge,Jorge Cadillo,22",
                "Martin,Martin Palos,24",//satisfy
                "Paco,Paco Palos,32",
                "Susy,Susy Palos,34",
                "Marina,Marina Palos,25"//satisfy
        );
        StructType types = new StructType()
                .add(DataTypes.createStructField(PlayerInput.shortName.getName(), StringType, true))
                .add(DataTypes.createStructField(PlayerInput.longName.getName(), StringType, true))
                .add(DataTypes.createStructField(PlayerInput.age.getName(), StringType, true));
        transformer = new TransformerExercise(spark, spark.createDataFrame(getListRow(lineas), types));
        transformer.addAgeRange();
        Assert.assertEquals(2, transformer.getDataframe().filter(PlayerOutput.ageRange.column().equalTo("B")).count());
    }

    //endregion

    @After
    public void tearDown() {
        spark.close();
    }

    private List<Row> getListRow(List<String> lines) {
        return lines.stream().map(e -> RowFactory.create((Object[]) e.split(","))).collect(Collectors.toList());
    }

    private StructType getStructType() {
        return new StructType()
                .add(DataTypes.createStructField(PlayerInput.shortName.getName(), StringType, true))
                .add(DataTypes.createStructField(PlayerOutput.ageRange.getName(), StringType, true))
                .add(DataTypes.createStructField(PlayerOutput.rankByNationalityPosition.getName(), StringType, true))
                .add(DataTypes.createStructField(PlayerOutput.potentialVsOverall.getName(), StringType, true));
    }

}
