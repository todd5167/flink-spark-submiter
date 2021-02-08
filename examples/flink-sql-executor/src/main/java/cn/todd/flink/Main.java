package cn.todd.flink;

import cn.todd.flink.runner.SqlRunner;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;


public class Main {

    public static void main(String[] args) throws Exception {
        final ParameterTool params = ParameterTool.fromArgs(args);
        String sqlPath = params.get("sqlFilePath");
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(sqlPath));

        SqlRunner submit = new SqlRunner(sqlPath);
        submit.run();
    }

}
