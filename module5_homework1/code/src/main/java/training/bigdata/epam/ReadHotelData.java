package training.bigdata.epam;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import training.bigdata.epam.ConstantsLoader.Constants;

import java.util.ArrayList;
import java.util.List;

public class ReadHotelData {

    public static Dataset<Row> readHotelData (SparkSession spark){

        // provide path to input text files
        String motelsPath = Driver.class.getResource("/motels.txt").getPath();

        // read text files to RDD
        JavaRDD<String> bidRDD = spark.sparkContext()
                .textFile(motelsPath, 1)
                .toJavaRDD();

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : Constants.hotelSchema.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD =
                bidRDD.map(new Function<String, Row>() {
                    @Override
                    public Row call(String record) throws Exception {
                        String[] attributes = record.split(",",-1);
                        return RowFactory.create(
                                attributes[0],
                                attributes[1],
                                attributes[2],
                                attributes[3],
                                attributes[4]
                        );
                    }
                });

        // Apply the schema to the RDD
        return spark.createDataFrame(rowRDD, schema);

    }


}
