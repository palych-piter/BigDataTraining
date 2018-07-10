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

public class ReadBidData {


    public static Dataset<Row> readBidData (SparkSession spark){

        // provide path to input text files
        String bidsPath = Driver.class.getResource("/bids.txt").getPath();

        // read bid text files to RDD
        JavaRDD<String> bidRDD = spark.sparkContext()
                .textFile(bidsPath, 1)
                .toJavaRDD();

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : Constants.bidSchema.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        JavaRDD<Row> rowRDD =
                bidRDD.filter(s -> !s.split(",")[2].contains("ERROR"))
                        .map(new Function<String, Row>() {
                            @Override
                            public Row call(String record) throws Exception {
                                String[] attributes = record.split(",",-1);
                                return RowFactory.create(
                                        attributes[0],
                                        attributes[1],
                                        attributes[2],
                                        attributes[3],
                                        attributes[4],
                                        attributes[5],
                                        attributes[6],
                                        attributes[7],
                                        attributes[8],
                                        attributes[9],
                                        attributes[10],
                                        attributes[11],
                                        attributes[12],
                                        attributes[13],
                                        attributes[14],
                                        attributes[15],
                                        attributes[16],
                                        attributes[17]
                                );
                            }
                        });

        // Apply the schema to the RDD
        return spark.createDataFrame(rowRDD, schema);

    }

}
