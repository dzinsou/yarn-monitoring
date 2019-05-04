package fr.dzinsou.yarnmonitoring.domain;

public class LogEvent {
    private String id;
    private Container containerInfo;
    private String logType;
    private String logLastModifiedTime;
    private Long logLength;
    private String logContent;
    private Long logLine;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Container getContainerInfo() {
        return containerInfo;
    }

    public void setContainerInfo(Container containerInfo) {
        this.containerInfo = containerInfo;
    }

    public String getLogType() {
        return logType;
    }

    public void setLogType(String logType) {
        this.logType = logType;
    }

    public String getLogLastModifiedTime() {
        return logLastModifiedTime;
    }

    public void setLogLastModifiedTime(String logLastModifiedTime) {
        this.logLastModifiedTime = logLastModifiedTime;
    }

    public Long getLogLength() {
        return logLength;
    }

    public void setLogLength(Long logLength) {
        this.logLength = logLength;
    }

    public String getLogContent() {
        return logContent;
    }

    public void setLogContent(String logContent) {
        this.logContent = logContent;
    }

    public Long getLogLine() {
        return logLine;
    }

    public void setLogLine(Long logLine) {
        this.logLine = logLine;
    }
}
