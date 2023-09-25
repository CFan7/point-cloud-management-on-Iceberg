package iceberg;

import breeze.util.partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Table;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.execution.columnar.NULL;

import java.util.Map;

public class SQL {

    SparkSession spark;
    String tableName;

    public SQL(SparkSession spark, String tableName) {
        this.spark = spark;
        this.tableName = tableName;
    }

    public Dataset<Row> reader() {
        // read the whole chart
        return spark.read()
                .format("iceberg")
                .load(tableName);
    }

    public Dataset<Row> reader(Long snapshotId) {
        // read base on the specific snapshotId
        return spark.read()
                .option("snapshot-id", snapshotId)
                .format("iceberg")
                .load(tableName);
    }

    public Dataset<Row> reader(String timestamp) {
        // read based on the timestamp, in milliseconds
        return spark.read()
                .option("as-of-timestamp", timestamp)
                .format("iceberg")
                .load(tableName);
    }

    public Dataset<Row> readIncrementally(Long startSnapshotId, Long endSnapshotId) {
        // read appended data incrementally
        return spark.read()
                .format("iceberg")
                .option("start-snapshot-id", startSnapshotId)
                .option("end-snapshot-id", endSnapshotId)
                .load(tableName);
    }

    public Dataset<Row> readPartitions() {
        // show the table’s current partitions, record counts, file counts
        // For unpartitioned tables,
        // the partitions table will contain only the record_count and file_count columns.
//        partition         record_count	file_count	spec_id
//        {20211001, 11}	1	            1	        0
//        {20211002, 11}	1	            1	        0
//        {20211001, 10}	1	            1	        0
//        {20211002, 10}	1	            1	        0

        return spark.read()
                .format("iceberg")
                .load(tableName + ".partitions");
    }

    // V1 API
    //        data.write()
    //                .mode("append")
    //                .option("location", "s3://iceberg/" + tableName)
    //                .saveAsTable(tableName);

    public void append(Dataset<Row> data, Map<String, String> properties) {
        // V2 API
        try {
            data.writeTo(tableName)
                    .options(properties)
                    .append();
        } catch (NoSuchTableException e) {
            e.printStackTrace();
        }
    }

    public void overwrite(Dataset<Row> data, Map<String, String> properties) {
        // V2 API
        try {
            data.writeTo(tableName)
                    .options(properties)
                    .overwritePartitions();
        } catch (NoSuchTableException e) {
            e.printStackTrace();
        }
    }

    public void delete(String restriction) {
        // 只支持了Spark sql
        String sqlContent = "DELETE FROM " + tableName + " WHERE " + restriction;
        spark.sql(sqlContent);
    }
}
