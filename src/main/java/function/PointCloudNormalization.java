package function;

import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.types.DataTypes;
import static org.apache.spark.sql.functions.*;

public class PointCloudNormalization extends Functions{

    public PointCloudNormalization(Dataset<Row> data) {
        super(data);
    }

    public Dataset<Row> normalize() {

        // 对于每个维度，计算均值和标准差
        double meanX = data.agg(avg(col("x"))).first().getDouble(0);
        double stdX = data.agg(stddev(col("x"))).first().getDouble(0);
        double meanY = data.agg(avg(col("y"))).first().getDouble(0);
        double stdY = data.agg(stddev(col("y"))).first().getDouble(0);
        double meanZ = data.agg(avg(col("z"))).first().getDouble(0);
        double stdZ = data.agg(stddev(col("z"))).first().getDouble(0);


        // 定义UDF以中心化和缩放每个点的每个维度
        UserDefinedFunction centerAndScaleX =
                functions.udf((Double x) -> (x - meanX) / stdX, DataTypes.DoubleType);

        data = data.withColumn("x", centerAndScaleX.apply(data.col("x")));

        UserDefinedFunction centerAndScaleY =
                functions.udf((Double y) -> (y - meanY) / stdY, DataTypes.DoubleType);

        data = data.withColumn("y", centerAndScaleY.apply(data.col("y")));

        UserDefinedFunction centerAndScaleZ =
                functions.udf((Double z) -> (z - meanZ) / stdZ, DataTypes.DoubleType);

        data = data.withColumn("z", centerAndScaleZ.apply(data.col("z")));

        return data;
    }
}
