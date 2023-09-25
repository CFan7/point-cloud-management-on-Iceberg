package iceberg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SnapshotManagement {

    SparkSession spark;
    String tableName;
    private static String catalogName = "my_catalog";

    public SnapshotManagement(SparkSession spark, String tableName) {
        this.spark = spark;
        this.tableName = tableName;
    }

    public Dataset<Row> readHistory() {
        // read the past snapshots with their relationships and set up time
        // e.g.
//        made_current_at	        snapshot_id	        parent_id	        is_current_ancestor
//        2019-02-08 03:29:51.215	5781947118336215154 NULL                true
//        2019-02-08 03:47:55.948	5179299526185056830	5781947118336215154	true
        return spark.read()
                .format("iceberg")
                .load(tableName + ".history");
    }

    public Dataset<Row> readSnapshots() {
        // show the valid snapshots for the table
//        committed_at	            snapshot_id	    parent_id	operation	manifest_list	summary
//        2019-02-08 03:29:51.215	57897183625154	null	    append	    s3://…/	        { added-records -> 2478404…}
        return spark.read()
                .format("iceberg")
                .load(tableName + ".snapshots");
    }

    public void rollbackToSnapshot(Long snapshotId) {
        // 目前只支持Spark sql进行操作
        String sqlContext = "CALL " + catalogName + ".system.rollback_to_snapshot('"
                + tableName + "', " + snapshotId + ");";
        spark.sql(sqlContext);
    }

    public void rollbackToTimestamp(String timestamp) {
        // 目前只支持Spark sql进行操作
        String sqlContext = "CALL " + catalogName + ".system.rollback_to_timestamp('" + tableName
                + "', TIMESTAMP '" + timestamp + "');";
        spark.sql(sqlContext);
    }

    public void expireSnapshot(String timestamp) {
        // 目前只支持Spark sql进行操作
        // 删除早于特定timestamp的快照
        String sqlContext = "CALL " + catalogName + ".system.expire_snapshots('" + tableName
                + "', TIMESTAMP '" + timestamp + "');";
        spark.sql(sqlContext);
    }
}
