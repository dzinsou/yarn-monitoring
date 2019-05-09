package fr.dzinsou.yarnmonitoring;

public class AppConfig {
    private String hadoopCluster;
    private String hadoopConf;
    private String hadoopUser;
    private String hadoopKeytab;

    private String[] outputTypeArray;
    private Integer outputBatchSize;
    private Integer outputBatchInterval;
    private Boolean outputAsync;

    private String outputKafkaConf;
    private String outputKafkaTopic;

    public String getHadoopCluster() {
        return hadoopCluster;
    }

    public void setHadoopCluster(String hadoopCluster) {
        this.hadoopCluster = hadoopCluster;
    }

    public String getHadoopConf() {
        return hadoopConf;
    }

    public void setHadoopConf(String hadoopConf) {
        this.hadoopConf = hadoopConf;
    }

    public String getHadoopUser() {
        return hadoopUser;
    }

    public void setHadoopUser(String hadoopUser) {
        this.hadoopUser = hadoopUser;
    }

    public String getHadoopKeytab() {
        return hadoopKeytab;
    }

    public void setHadoopKeytab(String hadoopKeytab) {
        this.hadoopKeytab = hadoopKeytab;
    }

    public String[] getOutputTypeArray() {
        return outputTypeArray;
    }

    public void setOutputTypeArray(String[] outputTypeArray) {
        this.outputTypeArray = outputTypeArray;
    }

    public Integer getOutputBatchSize() {
        return outputBatchSize;
    }

    public void setOutputBatchSize(Integer outputBatchSize) {
        this.outputBatchSize = outputBatchSize;
    }

    public Integer getOutputBatchInterval() {
        return outputBatchInterval;
    }

    public void setOutputBatchInterval(Integer outputBatchInterval) {
        this.outputBatchInterval = outputBatchInterval;
    }

    public Boolean getOutputAsync() {
        return outputAsync;
    }

    public void setOutputAsync(Boolean outputAsync) {
        this.outputAsync = outputAsync;
    }

    public String getOutputKafkaConf() {
        return outputKafkaConf;
    }

    public void setOutputKafkaConf(String outputKafkaConf) {
        this.outputKafkaConf = outputKafkaConf;
    }

    public String getOutputKafkaTopic() {
        return outputKafkaTopic;
    }

    public void setOutputKafkaTopic(String outputKafkaTopic) {
        this.outputKafkaTopic = outputKafkaTopic;
    }
}
