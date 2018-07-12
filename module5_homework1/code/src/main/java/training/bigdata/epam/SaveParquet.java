package training.bigdata.epam;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SaveParquet {
    public  static void saveParquet(Dataset<Row> datasetToSave, String path, String saveMode){
        datasetToSave
                .coalesce(1)
                .write()
                .mode(saveMode)
                .parquet(path)
        ;
    }
}


