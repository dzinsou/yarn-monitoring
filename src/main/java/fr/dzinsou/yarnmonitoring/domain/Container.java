package fr.dzinsou.yarnmonitoring.domain;

public class Container {
    private String containerId;
    private String appId;
    private String appName;
    private String appType;
    private String appState;
    private String appFinalStatus;
    private String appLogAggStatus;
    private String appAttemptId;
    private String queue;
    private String user;
    private String assignedNodeId;
    private String assignedNodeHttpAddress;
    private Long allocatedMemorySize;
    private Integer allocatedVirtualCores;
    private Integer containerExitStatus;
    private String containerState;
    private Long creationTime;
    private Long finishTime;
    private String logUrl;

    private Integer priority;

    public String getContainerId() {
        return containerId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public String getAppId() {
        return appId;
    }

    public void setAppId(String appId) {
        this.appId = appId;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getAppType() {
        return appType;
    }

    public void setAppType(String appType) {
        this.appType = appType;
    }

    public String getAppState() {
        return appState;
    }

    public void setAppState(String appState) {
        this.appState = appState;
    }

    public String getAppFinalStatus() {
        return appFinalStatus;
    }

    public void setAppFinalStatus(String appFinalStatus) {
        this.appFinalStatus = appFinalStatus;
    }

    public String getAppLogAggStatus() {
        return appLogAggStatus;
    }

    public void setAppLogAggStatus(String appLogAggStatus) {
        this.appLogAggStatus = appLogAggStatus;
    }

    public String getAppAttemptId() {
        return appAttemptId;
    }

    public void setAppAttemptId(String appAttemptId) {
        this.appAttemptId = appAttemptId;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getAssignedNodeId() {
        return assignedNodeId;
    }

    public void setAssignedNodeId(String assignedNodeId) {
        this.assignedNodeId = assignedNodeId;
    }

    public String getAssignedNodeHttpAddress() {
        return assignedNodeHttpAddress;
    }

    public void setAssignedNodeHttpAddress(String assignedNodeHttpAddress) {
        this.assignedNodeHttpAddress = assignedNodeHttpAddress;
    }

    public Long getAllocatedMemorySize() {
        return allocatedMemorySize;
    }

    public void setAllocatedMemorySize(Long allocatedMemorySize) {
        this.allocatedMemorySize = allocatedMemorySize;
    }

    public Integer getAllocatedVirtualCores() {
        return allocatedVirtualCores;
    }

    public void setAllocatedVirtualCores(Integer allocatedVirtualCores) {
        this.allocatedVirtualCores = allocatedVirtualCores;
    }

    public Integer getContainerExitStatus() {
        return containerExitStatus;
    }

    public void setContainerExitStatus(Integer containerExitStatus) {
        this.containerExitStatus = containerExitStatus;
    }

    public String getContainerState() {
        return containerState;
    }

    public void setContainerState(String containerState) {
        this.containerState = containerState;
    }

    public Long getCreationTime() {
        return creationTime;
    }

    public void setCreationTime(Long creationTime) {
        this.creationTime = creationTime;
    }

    public Long getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(Long finishTime) {
        this.finishTime = finishTime;
    }

    public String getLogUrl() {
        return logUrl;
    }

    public void setLogUrl(String logUrl) {
        this.logUrl = logUrl;
    }

    public Integer getPriority() {
        return priority;
    }

    public void setPriority(Integer priority) {
        this.priority = priority;
    }
}
