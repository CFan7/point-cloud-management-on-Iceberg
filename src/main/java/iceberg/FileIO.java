package iceberg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class FileIO {

    private static String outputRoot = "/home/ubuntu/final-essay/share/";

    public void toCSVFile(Dataset<Row> data, String outputFolder) {

        String outputPath = outputRoot + outputFolder;

        data.coalesce(1) // 合并为一个分区, 即生成一个文件
                .write()
                .option("header", "false")
                .mode("overwrite")
                .format("csv")
                .save(outputPath);

    }

    public void toXYZFile(Dataset<Row> data, String outputPath) {
        data.select(data.col("x"), data.col("y"), data.col("z"))
                .coalesce(1)
                .write()
                .format("csv")
                .option("header", "false")
                .mode("overwrite")
                .save(outputPath);
    }
}
