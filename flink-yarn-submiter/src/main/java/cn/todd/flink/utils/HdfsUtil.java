package cn.todd.flink.utils;

import cn.todd.flink.entity.CheckpointInfo;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class HdfsUtil {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

    /**
     * find file path in dir
     *
     * @param fileSystem
     * @param path
     * @return
     */
    public static List<CheckpointInfo> listFiles(
            FileSystem fileSystem, Path path, PathFilter filter) {
        List<CheckpointInfo> filePathsAndModifyTime = Lists.newArrayList();
        try {
            FileStatus[] fileIterator = fileSystem.listStatus(path, filter);
            /** file name order by ModificationTime desc */
            filePathsAndModifyTime =
                    Stream.of(fileIterator)
                            .filter(fileStatus -> isValidCheckpoint(fileSystem, fileStatus))
                            .sorted(
                                    (a, b) ->
                                            (int)
                                                    (b.getModificationTime()
                                                            - a.getModificationTime()))
                            .map(
                                    fileStatus ->
                                            new CheckpointInfo(
                                                    fileStatus.getPath().toString(),
                                                    fileStatus.getModificationTime()))
                            .collect(Collectors.toList());

        } catch (IOException e) {
            LOG.error("list file error!", e);
        }
        return filePathsAndModifyTime;
    }

    private static boolean isValidCheckpoint(FileSystem fs, FileStatus fileStatus) {
        try {
            Path metadata = new Path(fileStatus.getPath(), "_metadata");
            FileStatus status = fs.getFileStatus(metadata);
            LOG.info(
                    "Checkpoint dir {} has metadata, file length is:{}",
                    fileStatus.getPath(),
                    status.getLen());
            return true;
        } catch (Exception e) {
            LOG.error(
                    "Cannot find metadata file in directory {}.Please try to load the checkpoint/savepoint directly from the metadata file instead of the directory.",
                    fileStatus.getPath());
            return false;
        }
    }
}
