package translator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import system.CommandRunner;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.System.exit;


public class LASFileToIceberg {

    public static void main(String[] args) {

        String inputFilePath = args[0];
        String tableName = args[1];

        File folder = new File(inputFilePath);
        CommandRunner commandRunner = new CommandRunner();

        traverseFolder(inputFilePath, folder, commandRunner);

        csvFolder(inputFilePath, tableName);
    }

    public static void traverseFolder(String inputFilePath, File folder, CommandRunner commandRunner) {
        if (folder == null || !folder.exists()) {
            System.out.println("inputFilePath does not exist!");
            exit(1);
            return;
        }

        File[] files = folder.listFiles(new FilenameFilter() {
            public boolean accept(File dir, String name) {
                return name.toLowerCase().endsWith(".las");
            }
        });

        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    traverseFolder(inputFilePath, file, commandRunner);
                } else {
                    String fileNamePrefix = file.getName().replaceFirst("[.][^.]+$", "");
                    try {
                        commandRunner.pdalCliRunner("translate",
                                inputFilePath + file.getName(),
                                inputFilePath + fileNamePrefix + ".csv");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }


    private static void csvFolder(String inputFilePath, String tableName) {

        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Write LAS to Iceberg Table")
                .getOrCreate();

        // 定义Schema
        StructType structType = new StructType()
                .add("x", DataTypes.DoubleType)
                .add("y", DataTypes.DoubleType)
                .add("z", DataTypes.DoubleType)
                .add("intensity", DataTypes.DoubleType)
                .add("returnNumber", DataTypes.DoubleType)
                .add("numberOfReturns", DataTypes.DoubleType)
                .add("scanDirectionFlag", DataTypes.DoubleType)
                .add("edgeOfFlightLine", DataTypes.DoubleType)
                .add("classification", DataTypes.DoubleType)
                .add("scanAngleRank", DataTypes.DoubleType)
                .add("userData", DataTypes.DoubleType)
                .add("pointSourceId", DataTypes.DoubleType)
                .add("red", DataTypes.DoubleType)
                .add("green", DataTypes.DoubleType)
                .add("blue", DataTypes.DoubleType);

        // 读取数据并将其转换为DataFrame
        Dataset<Row> data = spark.read()
                .option("header", "true")
                .option("inferSchema", "false")
                .schema(structType)
                .csv(inputFilePath + "/*.csv");

        Map<String, String> properties = new HashMap<>();
        properties.put("iceberg.spatial.partitioning.scheme", "zorder");
        properties.put("iceberg.spatial.zorder.bits", "20"); // 控制精度的位数，越高越精确

        data.write()
                .mode("append")
                .option("location", "s3://iceberg/" + tableName)
                .options(properties)
                .saveAsTable(tableName);
    }

    private static void batchFile(String inputFilePath, String tableName) {
        SparkConf conf = new SparkConf().setAppName("Read LAS Files to Iceberg");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SparkSession spark = SparkSession.builder()
                .appName("Write LAS to Iceberg Table")
                .getOrCreate();

        // 读取多个文本文件
        // inputFiles是一个JavaPairRDD<String, String>类型的对象, key: 文件路径, value: 文件内容
        JavaPairRDD<String, String> inputFiles = jsc.wholeTextFiles(inputFilePath);

        JavaRDD<LasRecord> lasRecordsRDD = inputFiles.flatMap(fileContent -> {
            String[] lines = fileContent._2().split("\n");
            List<LasRecord> records = new ArrayList<>();

            // i从1开始, 跳过文件header
            for (int i = 1; i < lines.length; i++) {
                String line = lines[i];
                String[] fields = line.split(",");
                if (fields.length != 13 && fields.length != 16) {
                    System.out.println("解析属性列表失败: 属性数量错误！");
                    exit(1);
                }
                double x = Double.parseDouble(fields[0]);
                double y = Double.parseDouble(fields[1]);
                double z = Double.parseDouble(fields[2]);
                int intensity = (int)Double.parseDouble(fields[3]);
                int returnNumber = (int)Double.parseDouble(fields[4]);
                int numberOfReturns = (int)Double.parseDouble(fields[5]);
                int scanDirectionFlag = (int)Double.parseDouble(fields[6]);
                int edgeOfFlightLine = (int)Double.parseDouble(fields[7]);
                int classification = (int)Double.parseDouble(fields[8]);
                int scanAngleRank = (int)Double.parseDouble(fields[9]);
                int userData = (int)Double.parseDouble(fields[10]);
                int pointSourceId = (int)Double.parseDouble(fields[11]);
                double gpsTime = Double.parseDouble(fields[12]);
                int red = 0;
                int green = 0;
                int blue = 0;
                if (fields.length == 16) {  // 如果包含RGB属性，解析RGB值
                    red = (int)Double.parseDouble(fields[13]);
                    green = (int)Double.parseDouble(fields[14]);
                    blue = (int)Double.parseDouble(fields[15]);
                }
                LasRecord record = new LasRecord(x, y, z, intensity, returnNumber, numberOfReturns,
                        scanDirectionFlag, edgeOfFlightLine, classification, scanAngleRank,
                        userData, pointSourceId, gpsTime, red, green, blue);  // 创建LasRecord对象
                records.add(record);
            }
            return records.iterator();
        });

        Dataset<Row> df = spark.createDataFrame(lasRecordsRDD, LasRecord.class);
        System.out.println("下面即将打印df的schema");
        df.printSchema();
        System.out.println(df.count());

//        Map<String, String> properties = new HashMap<>();
//        properties.put("iceberg.spatial.partitioning.scheme", "zorder");
//        properties.put("iceberg.spatial.zorder.bits", "20"); // 控制精度的位数，越高越精确
//
//        // 将数据写入Iceberg表中
//        df.write()
//                .mode("append")
//                .option("location", "s3://iceberg/" + tableName)
//                .options(properties)
//                .saveAsTable(tableName);
    }
}


