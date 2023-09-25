package function;

import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSH;
import org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.IntegerType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class DownSampling extends Functions{

    public DownSampling(Dataset<Row> data) {
        super(data);
    }

    /**
     * 均匀采样
     * @param numSamples 采样点数量
     * @return 采样后的点云数据
     */

    public Dataset<Row> uniformSampling(int numSamples) {
        // 获取点云数据的数量
        long count = data.count();
        System.out.println("!!! [uniformSampling] number of points in source data: " + count);

        // 如果采样点数量大于点云数据数量，则直接返回原始数据
        if (numSamples >= count) {
            return data;
        }

        // 计算采样步长
        double step = (double) count / numSamples;

        // 添加id列
        data = data.withColumn("id", functions.monotonically_increasing_id());

        // 获取采样点
        // Math.round(step)是对步长进行四舍五入，然后将id列对该步长取余，得到的结果为0的点被保留下来，而其他点则被过滤掉。这样就实现了均匀采样的效果。
        Dataset<Row> samples = data
                .filter(functions.expr("id % " + (int) Math.round(step) + " = 0"))
                .drop("id");

        System.out.println("!!! [uniformSampling] number of points in result data: " + samples.count());


        return samples;
    }

    /**
     * 体素采样
     * @param gridSize 网格边长的数量级 // 对于lion, 设置gridSize为100时能得到24370个点
     * @return 采样后的点云数据
     */

    public Dataset<Row> voxelSampling(double gridSize) {

        System.out.println("!!! [voxelSampling] number of points in source data: " + data.count());

        double xMin = data.agg(functions.min("x")).first().getDouble(0);
        double yMin = data.agg(functions.min("y")).first().getDouble(0);
        double zMin = data.agg(functions.min("z")).first().getDouble(0);
        double xMax = data.agg(functions.max("x")).first().getDouble(0);
        double yMax = data.agg(functions.max("y")).first().getDouble(0);
        double zMax = data.agg(functions.max("z")).first().getDouble(0);

        double xRange = xMax - xMin;
        double yRange = yMax - yMin;
        double zRange = zMax - zMin;

        // 计算每个格子的大小
        double gridSizeX = xRange / gridSize;
        double gridSizeY = yRange / gridSize;
        double gridSizeZ = zRange / gridSize;

        // 定义UDF，将点的坐标转换为网格坐标
        UserDefinedFunction toGridCoordinate =
                functions.udf((Double x, Double y, Double z) -> {
                    int xGrid = (int) Math.floor((x - xMin) / gridSizeX);
                    int yGrid = (int) Math.floor((y - yMin) / gridSizeY);
                    int zGrid = (int) Math.floor((z - zMin) / gridSizeZ);
                    return xGrid + "," + yGrid + "," + zGrid;
                }, DataTypes.StringType);

        // 将点云数据按照网格坐标进行分组，取每个网格中的第一个点
        Dataset<Row> gridPoints = data
                .withColumn("grid_coordinate",
                        toGridCoordinate.apply(data.col("x"), data.col("y"), data.col("z")))
                .groupBy("grid_coordinate")
                .agg(functions.first("x"),
                        functions.first("y"),
                        functions.first("z"),
                        functions.first("intensity"),
                        functions.first("returnNumber"),
                        functions.first("numberOfReturns"),
                        functions.first("scanDirectionFlag"),
                        functions.first("edgeOfFlightLine"),
                        functions.first("classification"),
                        functions.first("scanAngleRank"),
                        functions.first("userData"),
                        functions.first("pointSourceId"),
                        functions.first("red"),
                        functions.first("green"),
                        functions.first("blue"))
                .drop("grid_coordinate");

        System.out.println("!!! [voxelSampling] number of points in result data: " + gridPoints.count());

        return gridPoints;
    }

    /**
     * 随机采样
     * @param numSamples 采样点数量
     * @return 采样后的点云数据
     */

    public Dataset<Row> randomSampling(int numSamples) {

        // 获取点云数据的数量
        long count = data.count();
        System.out.println("!!! [randomSampling] number of points in source data: " + count);


        // 如果采样点数量大于点云数据数量，则直接返回原始数据
        if (numSamples >= count) {
            return data;
        }

        // 对原始数据进行随机采样
        Dataset<Row> samples = data.orderBy(functions.rand()).limit(numSamples);

        System.out.println("!!! [randomSampling] number of points in result data: " + samples.count());

        return samples;
    }

    /**
     * 最远点采样
     * @param numSamples 采样点数量
     * @return 采样后的点云数据
     */

    public Dataset<Row> farthestPointSampling(SparkSession spark, int numSamples) {

        // 获取点云数据的数量
        long count = data.count();
        System.out.println("!!! [farthestPointSampling] number of points in source data: " + count);

        // 如果采样点数量大于点云数据数量，则直接返回原始数据
        if (numSamples >= count) {
            return data;
        }

        // 新增id列
        data = data.withColumn("id", functions.monotonically_increasing_id());

        // 新增distance列
        Dataset<Row> dataWithDistance = data.withColumn("distance", functions.lit(Double.MAX_VALUE));

        // 选取dataWithDistance的子集参与后续运算
        Dataset<Row> calculationData = dataWithDistance.select("x", "y", "z", "id", "distance");

        // 创建结果集
        List<Row> samples = new ArrayList<>();

        // 记录上一次新加入samples的点
        Row latestAddedPoint = calculationData.first();

        // 排除第一个采样点
        calculationData = calculationData
                .filter(calculationData.col("id").notEqual(latestAddedPoint.getLong(3)));

        // 找到distance最大的点
        ReduceFunction<Row> maxDistance = new ReduceFunction<Row>() {
            public Row call(Row a, Row b) throws Exception {
                double distanceA = a.getDouble(a.fieldIndex("distance"));
                double distanceB = b.getDouble(b.fieldIndex("distance"));
                if (distanceA > distanceB) {
                    return a;
                } else {
                    return b;
                }
            }
        };

        // 选择剩余采样点，每个点都与当前采样点计算距离，选出距离最远的点作为下一个采样点
        for (int i = 1; i < numSamples; i++) {
            // 将数据集持久化
            calculationData.persist(StorageLevel.MEMORY_AND_DISK());

            // 找到上一次加入采样点的点
            double lastX = latestAddedPoint.getDouble(0);
            double lastY = latestAddedPoint.getDouble(1);
            double lastZ = latestAddedPoint.getDouble(2);

            Column xDiff = functions.col("x").minus(lastX);
            Column yDiff = functions.col("y").minus(lastY);
            Column zDiff = functions.col("z").minus(lastZ);

            Column distance = xDiff.multiply(xDiff)
                    .plus(yDiff.multiply(yDiff))
                    .plus(zDiff.multiply(zDiff));

            // 计算距离
            calculationData = calculationData.withColumn("distance",
                    functions.when(calculationData.col("distance").lt(distance),
                            calculationData.col("distance")).otherwise(distance)
            );

            // 找到距离最远的点
            latestAddedPoint = calculationData.reduce(maxDistance);

            // 将距离最远的点加入到采样点中
            samples.add(latestAddedPoint);

            // 将距离最远的点从数据集中删除
            calculationData = calculationData
                    .filter(calculationData.col("id").notEqual(latestAddedPoint.getLong(3)));

            calculationData.unpersist();
        }

        StructType structType = new StructType()
                .add("x", DataTypes.DoubleType)
                .add("y", DataTypes.DoubleType)
                .add("z", DataTypes.DoubleType)
                .add("id", DataTypes.LongType)
                .add("distance", DataTypes.DoubleType);

        Dataset<Row> results = spark.createDataFrame(samples, structType);

        // 恢复到原始格式
        results = results.select("id");

        results = results.join(dataWithDistance, "id");

        results = results.drop("id").drop("distance");

        System.out.println("!!! [farthestPointSampling] number of points in result data: " + results.count());

        return results;
    }
}
