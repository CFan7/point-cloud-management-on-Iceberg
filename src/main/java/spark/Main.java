package spark;


import function.DataAugmentation;
import function.DownSampling;
import function.NeighborhoodSearch;
import function.PointCloudNormalization;
import iceberg.FileIO;
import iceberg.SQL;
import iceberg.SnapshotManagement;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;

import java.util.HashMap;
import java.util.Map;


public class Main {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().getOrCreate();

        // 4. query with functions
        String tableName = args[0]; // "lion_takanawa"

        Dataset<Row> data = null;
        String readMethod = args[1];
        if (readMethod.equals("none")) {
            data = new SQL(spark, tableName).reader();
        }
        else if (readMethod.equals("read_with_snapshot")) {
            long snapshotId = Long.parseLong(args[2]);
            data = new SQL(spark, tableName).reader(snapshotId);
        }
        else if (readMethod.equals("read_with_timestamp")) {
            String timestamp = args[2];
            data = new SQL(spark, tableName).reader(timestamp);
        }
        else if (readMethod.equals("readIncrementally")) {
            long startSnapshotId = Long.parseLong(args[2]);
            long endSnapshotId = Long.parseLong(args[3]);
            data = new SQL(spark, tableName).readIncrementally(startSnapshotId, endSnapshotId);
        }
        else {
            System.out.println("[Main] Unknown read method!");
            System.exit(1);
        }

        if (data == null) {
            System.out.println("[Main] Cannot read anything!");
            System.exit(1);
        }

        data = data.select("x", "y", "z");

        String operation = args[4];
        String operationMethod = args[5];

        if (operation.equals("down_sampling")) {
            DownSampling downSampling = new DownSampling(data);
            if (operationMethod.equals("uniform_sampling")) {
                int numSamples = Integer.parseInt(args[6]); // 68400
                data = downSampling.uniformSampling(numSamples);
            }
            else if (operationMethod.equals("voxel_sampling")) {
                double gridSize = Double.parseDouble(args[6]);
                data = downSampling.voxelSampling(gridSize);
            }
            else if (operationMethod.equals("random_sampling")) {
                int numSamples = Integer.parseInt(args[6]);
                data = downSampling.randomSampling(numSamples);
            }
            else if (operationMethod.equals("farthest_point_sampling")) {
                int numSamples = Integer.parseInt(args[6]);
                data = downSampling.farthestPointSampling(spark, numSamples);
            }
            else {
                System.out.println("[Main.downSampling] Unknown specific method !");
                System.exit(1);
            }
        }
        else if (operation.equals("neighborhood_search")) {
            // -3.355, 4.122, -1.932
            double x = Double.parseDouble(args[6]);
            double y = Double.parseDouble(args[7]);
            double z = Double.parseDouble(args[8]);
            NeighborhoodSearch ns = new NeighborhoodSearch(data, x, y, z);

            if (operationMethod.equals("KNN_search")) {
                int k = Integer.parseInt(args[9]);
                data = ns.KNNSearch(k);
            }
            else if (operationMethod.equals("KNN_search_with_color")) {
                int k = Integer.parseInt(args[9]);
                data = ns.KNNSearchWithColor(k);
            }
            else if (operationMethod.equals("ball_query")) {
                double distance = Double.parseDouble(args[9]); // 0.8
                data = ns.ballQuery(distance);
            }
            else if (operationMethod.equals("ball_query_with_color")) {
                double distance = Double.parseDouble(args[9]);
                data = ns.ballQueryWithColor(distance);
            }
            else {
                System.out.println("[Main.NeighborhoodSearch] Unknown specific method !");
                System.exit(1);
            }
        }
        else if (operation.equals("normalize")) {
            PointCloudNormalization pcn = new PointCloudNormalization(data);
            data = pcn.normalize();
        }
        else if (operation.equals("data_augmentation")) {
            DataAugmentation dataAugmentation = new DataAugmentation(data);
            if (operationMethod.equals("rotate")) {
                double rotationAngle = Double.parseDouble(args[6]);
                data = dataAugmentation.rotatePointCloud(rotationAngle);
            }
            else if (operationMethod.equals("jitter")) {
                double sigma = Double.parseDouble(args[6]);
                double clip = Double.parseDouble(args[7]);
                data = dataAugmentation.jitterPointCloud(sigma, clip);
            }
            else {
                System.out.println("[Main.DataAugmentation] Unknown specific method !");
                System.exit(1);
            }
        }
        else if (operation.equals("none")) {
        }
        else {
            System.out.println("[Main] Unknown operation !");
            System.exit(1);
        }

        new FileIO().toCSVFile(data, "lion-takanawa");

        spark.stop();


        // 1. upsert the table
//        String tableName = args[0];
//        String operation = args[1];
//
//        SQL sql = new SQL(spark, tableName);
//
//        if (operation.equals("append")) {
//            String inputPath = args[2];
//            Dataset<Row> data = spark.read().csv(inputPath);
//
//            Map<String, String> properties = new HashMap<>();
//            properties.put("iceberg.spatial.partitioning.scheme", "zorder");
//            properties.put("iceberg.spatial.zorder.bits", "20"); // 控制精度的位数，越高越精确
//
//            sql.append(data, properties);
//        }
//        else if (operation.equals("overwrite")) {
//            String inputPath = args[2];
//            Dataset<Row> data = spark.read().csv(inputPath);
//
//            Map<String, String> properties = new HashMap<>();
//            properties.put("iceberg.spatial.partitioning.scheme", "zorder");
//            properties.put("iceberg.spatial.zorder.bits", "20"); // 控制精度的位数，越高越精确
//
//            sql.overwrite(data, properties);
//        }
//        else if (operation.equals("delete")) {
//            String filter = args[2];
//            sql.delete(filter);
//        }


        // 2.show insight of a table
//        String tableName = args[0];
//        String operation = args[1];
//        String outputPath = "/home/ubuntu/final-essay/share/table-insight/";
//
//        if (operation.equals("read_history")) {
//            SnapshotManagement sm = new SnapshotManagement(spark, tableName);
//            sm.readHistory()
//                    .coalesce(1)
//                    .write()
//                    .mode("overwrite")
//                    .csv(outputPath);
//        }
//        else if (operation.equals("read_snapshot")) {
//            SnapshotManagement sm = new SnapshotManagement(spark, tableName);
//            sm.readSnapshots()
//                    .select("committed_at", "snapshot_id", "parent_id", "operation")
//                    .coalesce(1)
//                    .write()
//                    .mode("overwrite")
//                    .csv(outputPath);
//        }
//        else if (operation.equals("read_partition")) {
//            SQL sql = new SQL(spark, tableName);
//            sql.readPartitions()
//                    .coalesce(1)
//                    .write()
//                    .mode("overwrite")
//                    .csv(outputPath);
//        }
//        else {
//            System.out.println("[Main.snapshotManagement] Unknown operation!");
//            System.exit(1);
//        }


        // 3. show table lists
//        spark.sql("show tables;")
//                .select("tableName")
//                .coalesce(1)
//                .write()
//                .mode("overwrite")
//                .csv("/home/ubuntu/final-essay/share/table-lists");



    }
}