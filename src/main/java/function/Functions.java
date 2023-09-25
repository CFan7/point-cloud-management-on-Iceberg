package function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Functions {
    public Dataset<Row> data;

    public Functions(Dataset<Row> data) {
        this.data = data;
    }
}