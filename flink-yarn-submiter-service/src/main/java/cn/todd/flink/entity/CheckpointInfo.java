package cn.todd.flink.entity;

/**
 * Date: 2021/10/1
 *
 * @author todd5167
 */
public class CheckpointInfo {
    private String checkpointPath;
    private long createTime;

    public CheckpointInfo(String checkpointPath, long createTime) {
        this.checkpointPath = checkpointPath;
        this.createTime = createTime;
    }

    public String getCheckpointPath() {
        return checkpointPath;
    }

    public void setCheckpointPath(String checkpointPath) {
        this.checkpointPath = checkpointPath;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    @Override
    public String toString() {
        return "CheckpointInfo{"
                + "checkpointPath='"
                + checkpointPath
                + '\''
                + ", createTime="
                + createTime
                + '}';
    }
}
