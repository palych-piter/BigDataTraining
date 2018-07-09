package training.bigdata.epam;

import org.apache.spark.sql.Dataset;

public class SaveCSV {

    public  static void saveCsv(Dataset datasetToSave, String path, String saveMode){
        datasetToSave
                .coalesce(1)
                .write()
                .format("com.databricks.spark.csv")
                .option("header", true)
                .option("delimiter", ",")
                .mode(saveMode)
                .save(path);
    }

}


