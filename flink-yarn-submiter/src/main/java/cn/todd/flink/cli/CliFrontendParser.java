package cn.todd.flink.cli;

import org.apache.flink.util.StringUtils;
import org.apache.flink.util.function.FunctionUtils;

import cn.todd.flink.entity.ParamsInfo;
import cn.todd.flink.utils.JsonUtils;
import org.apache.commons.cli.*;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class CliFrontendParser {
    private DefaultParser parser = new DefaultParser();
    private Options options = new Options();
    private ParamsInfo.Builder builder = ParamsInfo.builder();

    public ParamsInfo parseParamsInfo(String[] args) throws ParseException {
        Field[] fields = SubmitterOptions.class.getDeclaredFields();

        Arrays.stream(fields)
                .forEach(
                        FunctionUtils.uncheckedConsumer(
                                field -> {
                                    options.addOption((Option) field.get(SubmitterOptions.class));
                                }));
        CommandLine commandLine = parser.parse(options, args);

        Optional.ofNullable(commandLine.getOptionValue(SubmitterOptions.NAME_OPTION.getOpt()))
                .ifPresent(builder::setName);

        Optional.ofNullable(
                        commandLine.getOptionValue(SubmitterOptions.RUN_JAR_PATH_OPTION.getOpt()))
                .ifPresent(builder::setRunJarPath);

        Optional.ofNullable(
                        commandLine.getOptionValue(SubmitterOptions.FLINK_CONF_DIR_OPTION.getOpt()))
                .ifPresent(builder::setFlinkConfDir);

        Optional.ofNullable(
                        commandLine.getOptionValue(SubmitterOptions.FLINK_JAR_PATH_OPTION.getOpt()))
                .ifPresent(builder::setFlinkJarPath);

        Optional.ofNullable(
                        commandLine.getOptionValue(
                                SubmitterOptions.HADOOP_CONF_DIR_OPTION.getOpt()))
                .ifPresent(builder::setHadoopConfDir);

        Optional.ofNullable(commandLine.getOptionValue(SubmitterOptions.QUEUE_OPTION.getOpt()))
                .ifPresent(builder::setQueue);

        Optional.ofNullable(
                        commandLine.getOptionValue(
                                SubmitterOptions.ENTRY_POINT_CLASSNAME_OPTION.getOpt()))
                .ifPresent(builder::setEntryPointClassName);

        Optional.ofNullable(commandLine.getOptionValue(SubmitterOptions.EXEC_ARGS_OPTION.getOpt()))
                .map(execArgs -> JsonUtils.parseJsonList(execArgs, String.class))
                .ifPresent(list -> builder.setExecArgs(list.toArray(new String[0])));

        Optional.ofNullable(
                        commandLine.getOptionValue(SubmitterOptions.DEPEND_FILES_OPTION.getOpt()))
                .map(ds -> JsonUtils.parseJsonList(ds, String.class))
                .map(this::addProtocolForFile)
                .ifPresent(files -> builder.setDependFiles(files.toArray(new String[0])));

        Optional.ofNullable(commandLine.getOptionValue(SubmitterOptions.FLINK_CONF_OPTION.getOpt()))
                .map(ds -> JsonUtils.parseJson(ds, Properties.class))
                .ifPresent(builder::setConfProperties);

        // --------security config  -------//
        String openSecurity =
                commandLine.getOptionValue(SubmitterOptions.OPEN_SECURITY_OPTION.getOpt());
        Optional.ofNullable(openSecurity).map(Boolean::valueOf).ifPresent(builder::setOpenSecurity);

        String keytabPath =
                commandLine.getOptionValue(SubmitterOptions.KEYTAB_PATH_OPTION.getOpt());
        Optional.ofNullable(keytabPath).ifPresent(builder::setKeytabPath);

        if (Boolean.valueOf(openSecurity) && StringUtils.isNullOrWhitespaceOnly(keytabPath)) {
            throw new ParseException("when open security,keytab file is required! ");
        }

        Optional.ofNullable(commandLine.getOptionValue(SubmitterOptions.KRB5_PATH_OPTION.getOpt()))
                .ifPresent(builder::setKrb5Path);

        Optional.ofNullable(commandLine.getOptionValue(SubmitterOptions.PRINCIPAL_OPTION.getOpt()))
                .ifPresent(builder::setPrincipal);

        Optional.ofNullable(
                        commandLine.getOptionValue(SubmitterOptions.APPLICATION_ID_OPTION.getOpt()))
                .ifPresent(builder::setApplicationId);

        return builder.build();
    }

    private List<String> addProtocolForFile(List<String> paths) {
        return paths.stream().map(path -> "file://" + path).collect(Collectors.toList());
    }
}
