package training.bigdata.epam;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExplodeBids {

    public static Dataset<Row> explodeBids(Dataset<Row> inputDataSet) {

        StructType schema = new StructType(new StructField[]{
                new StructField("date",
                        DataTypes.StringType, false,
                        Metadata.empty()),
                new StructField("motelId",
                        DataTypes.StringType, false,
                        Metadata.empty()),
                new StructField("LoSa",
                        DataTypes.StringType, false,
                        Metadata.empty()),
                new StructField("price",
                        DataTypes.StringType, false,
                        Metadata.empty())
        });

        ExpressionEncoder<Row> encoder = RowEncoder.apply(schema);

        Dataset<Row> outputDataSet = inputDataSet.flatMap(new FlatMapFunction<Row, Row>() {

            List<Row> returnList = new ArrayList<>();

            @Override
            public Iterator<Row> call(Row row) throws Exception {

                //replace blanks with nulls
                String usAmount = row.getAs("US").toString().isEmpty()?"0.00":row.getAs("US");
                String caAmount = row.getAs("CA").toString().isEmpty()?"0.00":row.getAs("CA");
                String mxAmount = row.getAs("MX").toString().isEmpty()?"0.00":row.getAs("MX");

                //generate rows
                //--- US
                returnList.add(RowFactory.create(
                        new String[]
                                {row.getAs("BidDate"),
                                 row.getAs("MotelID"),
                                 "US",
                                 usAmount,
                                }))
                ;
                //--- CA
                returnList.add(RowFactory.create(
                        new String[]
                                {row.getAs("BidDate"),
                                 row.getAs("MotelID"),
                                 "CA",
                                 caAmount,
                                }))
                ;
                //--- MX
                returnList.add(RowFactory.create(
                        new String[]
                                {row.getAs("BidDate"),
                                 row.getAs("MotelID"),
                                 "MX",
                                 mxAmount,
                                }))
                ;

                return returnList.iterator();

            }
        }, encoder);

        return outputDataSet.distinct();

    }
}
