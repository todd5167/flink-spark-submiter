package cn.todd.flink.entity;

public class ResultInfo {
    private String appId;
    private String jobId;
    private String msg;

    public ResultInfo(String appId, String jobId, String msg) {
        this.appId = appId;
        this.jobId = jobId;
        this.msg = msg;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    @Override
    public String toString() {
        return "ResultInfo{"
                + "appId='"
                + appId
                + '\''
                + ", jobId='"
                + jobId
                + '\''
                + ", msg='"
                + msg
                + '\''
                + '}';
    }
}
