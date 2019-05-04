package fr.dzinsou.yarnmonitoring;

public class AppConfig {
    private String hadoopConf;
    private String hadoopUser;
    private String hadoopKeytab;

    private String[] outputTypeArray;
    private Integer outputBatchSize;
    private Integer outputBatchInterval;
    private Boolean outputAsync;

    private String outputKafkaConf;
    private String outputKafkaTopic;

    private String[] outputElasticsearchHttpHostArray;
    private Integer outputElasticsearchHttpPort;
    private String outputElasticsearchHttpScheme;
    private String outputElasticsearchIndexPattern;
    private String outputElasticsearchDocType;
    private String outputElasticsearchRequestTimeout;

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

    public String[] getOutputElasticsearchHttpHostArray() {
        return outputElasticsearchHttpHostArray;
    }

    public void setOutputElasticsearchHttpHostArray(String[] outputElasticsearchHttpHostArray) {
        this.outputElasticsearchHttpHostArray = outputElasticsearchHttpHostArray;
    }

    public Integer getOutputElasticsearchHttpPort() {
        return outputElasticsearchHttpPort;
    }

    public void setOutputElasticsearchHttpPort(Integer outputElasticsearchHttpPort) {
        this.outputElasticsearchHttpPort = outputElasticsearchHttpPort;
    }

    public String getOutputElasticsearchHttpScheme() {
        return outputElasticsearchHttpScheme;
    }

    public void setOutputElasticsearchHttpScheme(String outputElasticsearchHttpScheme) {
        this.outputElasticsearchHttpScheme = outputElasticsearchHttpScheme;
    }

    public String getOutputElasticsearchIndexPattern() {
        return outputElasticsearchIndexPattern;
    }

    public void setOutputElasticsearchIndexPattern(String outputElasticsearchIndexPattern) {
        this.outputElasticsearchIndexPattern = outputElasticsearchIndexPattern;
    }

    public String getOutputElasticsearchDocType() {
        return outputElasticsearchDocType;
    }

    public void setOutputElasticsearchDocType(String outputElasticsearchDocType) {
        this.outputElasticsearchDocType = outputElasticsearchDocType;
    }

    public String getOutputElasticsearchRequestTimeout() {
        return outputElasticsearchRequestTimeout;
    }

    public void setOutputElasticsearchRequestTimeout(String outputElasticsearchRequestTimeout) {
        this.outputElasticsearchRequestTimeout = outputElasticsearchRequestTimeout;
    }
}
