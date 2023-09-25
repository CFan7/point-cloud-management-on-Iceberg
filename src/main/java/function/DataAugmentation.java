package function;

import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;

public class DataAugmentation extends Functions{

    public DataAugmentation(Dataset<Row> data) {
        super(data);
    }

    private Column rotatePoint(Column x, Column y, Column z, double rotationAngle) {

        double Pi = 3.1415926535;

        while (rotationAngle > 2 * Pi) {
            rotationAngle -= 2 * Pi;
        }

        while (rotationAngle < 0) {
            rotationAngle += 2 * Pi;
        }

        double cosTheta = Math.cos(rotationAngle);
        double sinTheta = Math.sin(rotationAngle);

        //        double[][] rotationMatrix = {{cosTheta, sinTheta, 0},
        //                                    {-sinTheta, cosTheta, 0},
        //                                    {0, 0, 1}};

        Column rotatedX = x.multiply(cosTheta).plus(x.multiply(sinTheta));
        Column rotatedY = y.multiply(-sinTheta).plus(y.multiply(cosTheta));

        return functions.struct(
                rotatedX.as("x"),
                rotatedY.as("y"),
                z.as("z")
        );
    }

    private Column jitterPoint(Column x, Column y, Column z, double sigma, double clip) {

        // randn() 方法返回一个标准正态分布（均值为 0，标准差为 1）的随机变量
        // multiply(sigma) 方法将其缩放为标准差为 sigma 的高斯分布
        Column jitteredX = functions.randn().multiply(sigma).plus(x);
        Column jitteredY = functions.randn().multiply(sigma).plus(y);
        Column jitteredZ = functions.randn().multiply(sigma).plus(z);

        // 保证jitteredX不会超过上限: x + clip
        jitteredX = functions.when(jitteredX.gt(x.plus(clip)), x.plus(clip))
                .otherwise(jitteredX);
        // 保证jitteredX不会低于下限: x - clip
        jitteredX = functions.when(jitteredX.lt(x.minus(clip)), x.minus(clip))
                .otherwise(jitteredX);

        jitteredY = functions.when(jitteredY.gt(y.plus(clip)), y.plus(clip))
                .otherwise(jitteredY);
        jitteredY = functions.when(jitteredY.lt(y.minus(clip)), y.minus(clip))
                .otherwise(jitteredY);

        jitteredZ = functions.when(jitteredZ.gt(z.plus(clip)), z.plus(clip))
                .otherwise(jitteredZ);
        jitteredZ = functions.when(jitteredZ.lt(z.minus(clip)), z.minus(clip))
                .otherwise(jitteredZ);

        return functions.struct(
                jitteredX.as("x"),
                jitteredY.as("y"),
                jitteredZ.as("z")
        );
    }

    /**
     * 绕Z轴旋转
     * @param rotationAngle 旋转角度
     * @return 绕Z轴旋转后的点云数据
     */

    public Dataset<Row> rotatePointCloud(double rotationAngle) {

        Column rotatedResult = rotatePoint(
                functions.col("x"),
                functions.col("y"),
                functions.col("z"), rotationAngle);

        Column rotatedX = rotatedResult.getField("x");
        Column rotatedY = rotatedResult.getField("y");

        return data
                .withColumn("x", rotatedX)
                .withColumn("y", rotatedY);
    }

    /**
     * 添加高斯噪声
     * @param sigma 高斯分布的标准差, 用于控制噪声的大小 e.g. 0.01
     * @param clip 用于截断噪声值的范围，确保添加的噪声不会太大或太小 e.g. 0.05
     * @return 添加高斯噪声后的点云数据
     */

    public Dataset<Row> jitterPointCloud(double sigma, double clip) {
        Column rotatedResult = jitterPoint(
                functions.col("x"),
                functions.col("y"),
                functions.col("z"), sigma, clip);

        Column rotatedX = rotatedResult.getField("x");
        Column rotatedY = rotatedResult.getField("y");
        Column rotatedZ = rotatedResult.getField("z");

        Dataset<Row> jitterPointCloud = data
                .withColumn("x", rotatedX)
                .withColumn("y", rotatedY)
                .withColumn("z", rotatedZ);

        return jitterPointCloud;
    }
}