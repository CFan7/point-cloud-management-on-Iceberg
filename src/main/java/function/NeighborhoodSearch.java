package function;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;

public class NeighborhoodSearch extends Functions{

    double x;
    double y;
    double z;

    public NeighborhoodSearch(Dataset<Row> data, double x, double y, double z) {
        super(data);
        this.x = x;
        this.y = y;
        this.z = z;
    }

    /**
     * 查询指定点周围最近的k个点
     * @param k 查询的k个临近点
     * @return 查询结果
     */

    public Dataset<Row> KNNSearch(int k) {

        Dataset<Row> targetRow = data
                .filter(functions.col("x").equalTo(x))
                .filter(functions.col("y").equalTo(y))
                .filter(functions.col("z").equalTo(z));

        if (targetRow.count() == 0) {
            System.out.println("[neighborhoodSearch] Could not find the target point in the table!");
            System.exit(1);
        }

        Column xDiff = functions.col("x").minus(x);
        Column yDiff = functions.col("y").minus(y);
        Column zDiff = functions.col("z").minus(z);

        Column distance = xDiff.multiply(xDiff)
                .plus(yDiff.multiply(yDiff))
                .plus(zDiff.multiply(zDiff));

        // 新增distance列
        data =  data.withColumn("distance", distance);
        targetRow = targetRow.withColumn("distance", functions.lit(0));

        Dataset<Row> results = data
                .except(targetRow)
                .orderBy("distance").limit(k);

        results = results.drop("distance");

        return results;
    }

    /**
     * 查询指定点周围最近的k个点并染色
     * @param k 查询的k个临近点
     * @return 查询结果
     */

    public Dataset<Row> KNNSearchWithColor(int k) {

        Dataset<Row> targetRow = data
                .filter(functions.col("x").equalTo(x))
                .filter(functions.col("y").equalTo(y))
                .filter(functions.col("z").equalTo(z));

        if (targetRow.count() == 0) {
            System.out.println("[neighborhoodSearch] Could not find the target point in the table!");
            System.exit(1);
        }

        Column xDiff = functions.col("x").minus(x);
        Column yDiff = functions.col("y").minus(y);
        Column zDiff = functions.col("z").minus(z);

        Column distance = xDiff.multiply(xDiff)
                .plus(yDiff.multiply(yDiff))
                .plus(zDiff.multiply(zDiff));

        // 新增distance列
        Dataset<Row> resultsPoints = data
                .except(targetRow)
                .withColumn("distance", distance)
                .orderBy("distance").limit(k)
                .drop("distance");

        Dataset<Row> otherPoints = data.except(targetRow).except(resultsPoints);

        // 上色
        // 将查询点的颜色设置为红色
        targetRow = targetRow
                .withColumn("red", functions.lit(255 * 255))
                .withColumn("green", functions.lit(0))
                .withColumn("blue", functions.lit(0));

        // 将查到的点颜色设置为绿色
        resultsPoints = resultsPoints
                .withColumn("red", functions.lit(34 * 255))
                .withColumn("green", functions.lit(139 * 255))
                .withColumn("blue", functions.lit(34 * 255));

        // 将其余点的颜色设置为灰蓝
        otherPoints = otherPoints
                .withColumn("red", functions.lit(245 * 255))
                .withColumn("green", functions.lit(245 * 255))
                .withColumn("blue", functions.lit(245 * 255));

        return targetRow.union(resultsPoints).union(otherPoints);
    }

    /**
     * 查询指定点周围特定距离内的点
     * @param distance 查询的k个临近点
     * @return 查询结果
     */

    public Dataset<Row> ballQuery(double distance) {

        Dataset<Row> targetRow = data
                .filter(functions.col("x").equalTo(x))
                .filter(functions.col("y").equalTo(y))
                .filter(functions.col("z").equalTo(z));

        if (targetRow.count() == 0) {
            System.out.println("[neighborhoodSearch] Could not find the target point in the table!");
            System.exit(1);
        }

        Column xDiff = functions.col("x").minus(x);
        Column yDiff = functions.col("y").minus(y);
        Column zDiff = functions.col("z").minus(z);

        Column distanceCol = functions.sqrt(xDiff.multiply(xDiff)
                .plus(yDiff.multiply(yDiff))
                .plus(zDiff.multiply(zDiff)));

        // 新增distance列
        data =  data.withColumn("distance", distanceCol);
        targetRow = targetRow.withColumn("distance", functions.lit(0));

        Dataset<Row> results = data
                .except(targetRow)
                .filter(functions.col("distance").lt(distance));

        results = results.drop("distance");

        return results;
    }

    /**
     * 查询指定点周围特定距离内的点并染色
     * @param distance 查询的k个临近点
     * @return 查询结果
     */

    public Dataset<Row> ballQueryWithColor(double distance) {

        Dataset<Row> targetRow = data
                .filter(functions.col("x").equalTo(x))
                .filter(functions.col("y").equalTo(y))
                .filter(functions.col("z").equalTo(z))
                .cache();

        if (targetRow.count() == 0) {
            System.out.println("[neighborhoodSearch] Could not find the target point in the table!");
            System.exit(1);
        }

        Column xDiff = functions.col("x").minus(x);
        Column yDiff = functions.col("y").minus(y);
        Column zDiff = functions.col("z").minus(z);

        Column distanceCol = functions.sqrt(xDiff.multiply(xDiff)
                .plus(yDiff.multiply(yDiff))
                .plus(zDiff.multiply(zDiff)));

        // 新增distance列
        Dataset<Row> resultsPoints = data
                .except(targetRow)
                .withColumn("distance", distanceCol)
                .filter(functions.col("distance").lt(distance))
                .drop("distance")
                .cache();

        Dataset<Row> otherPoints = data
                .withColumn("distance", distanceCol)
                .filter(functions.col("distance").gt(distance))
                .drop("distance");

        // 上色
        // 将查询点的颜色设置为红色
        targetRow = targetRow
                .withColumn("red", functions.lit(255 * 255))
                .withColumn("green", functions.lit(0))
                .withColumn("blue", functions.lit(0));

        // 将查到的点颜色设置为蓝色
        resultsPoints = resultsPoints
                .withColumn("red", functions.lit(0))
                .withColumn("green", functions.lit(0))
                .withColumn("blue", functions.lit(255 * 255));

        // 将其余点的颜色设置为灰蓝
        otherPoints = otherPoints
                .withColumn("red", functions.lit(245 * 255))
                .withColumn("green", functions.lit(245 * 255))
                .withColumn("blue", functions.lit(245 * 255));

        return targetRow.union(resultsPoints).union(otherPoints);
    }
}
