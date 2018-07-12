package training.bigdata.epam;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import training.bigdata.epam.ConstantsLoader.Constants;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ReadBidDataPar implements Serializable {

    public static Dataset<Row> readBidDataPar(SparkSession spark, String fileName) {

        Dataset<Row> _outputDf = spark.read().parquet(fileName);

        return _outputDf;

    }


}


