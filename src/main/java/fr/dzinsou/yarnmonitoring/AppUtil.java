package fr.dzinsou.yarnmonitoring;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class AppUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(AppUtil.class);

    public static AppConfig loadAppConfig(String appConfigFile) throws IOException {
        Properties appProperties = new Properties();
        try (FileReader fileReader = new FileReader(appConfigFile)) {
            appProperties.load(fileReader);
        }

        LOGGER.info("app.hadoop.conf                        : [{}]", appProperties.getProperty("app.hadoop.conf"));
        LOGGER.info("app.hadoop.user                        : [{}]", appProperties.getProperty("app.hadoop.user"));
        LOGGER.info("app.hadoop.keytab                      : [{}]",appProperties.getProperty("app.hadoop.keytab"));
        LOGGER.info("app.output.type                        : [{}]", appProperties.getProperty("app.output.type"));
        LOGGER.info("app.output.batch.size                  : [{}]", appProperties.getProperty("app.output.batch.size"));
        LOGGER.info("app.output.batch.interval              : [{}]", appProperties.getProperty("app.output.batch.interval"));
        LOGGER.info("app.output.async                       : [{}]", appProperties.getProperty("app.output.async"));
        LOGGER.info("app.output.kafka.conf                  : [{}]", appProperties.getProperty("app.output.kafka.conf"));
        LOGGER.info("app.output.kafka.topic                 : [{}]", appProperties.getProperty("app.output.kafka.topic"));
        LOGGER.info("app.output.elasticsearch.httpHosts     : [{}]", appProperties.getProperty("app.output.elasticsearch.httpHosts"));
        LOGGER.info("app.output.elasticsearch.httpPort      : [{}]", appProperties.getProperty("app.output.elasticsearch.httpPort"));
        LOGGER.info("app.output.elasticsearch.httpScheme    : [{}]", appProperties.getProperty("app.output.elasticsearch.httpScheme"));
        LOGGER.info("app.output.elasticsearch.indexPattern  : [{}]", appProperties.getProperty("app.output.elasticsearch.indexPattern"));
        LOGGER.info("app.output.elasticsearch.docType       : [{}]", appProperties.getProperty("app.output.elasticsearch.docType"));
        LOGGER.info("app.output.elasticsearch.requestTimeout: [{}]", appProperties.getProperty("app.output.elasticsearch.requestTimeout"));

        AppConfig appConfig = new AppConfig();
        appConfig.setHadoopConf(appProperties.getProperty("app.hadoop.conf"));
        appConfig.setHadoopUser(appProperties.getProperty("app.hadoop.user"));
        appConfig.setHadoopKeytab(appProperties.getProperty("app.hadoop.keytab"));
        appConfig.setOutputTypeArray(appProperties.getProperty("app.output.type").split(","));
        appConfig.setOutputBatchSize(Integer.valueOf(appProperties.getProperty("app.output.batch.size")));
        appConfig.setOutputBatchInterval(Integer.valueOf(appProperties.getProperty("app.output.batch.interval")));
        appConfig.setOutputAsync(Boolean.valueOf(appProperties.getProperty("app.output.async")));
        for (String outputType : appConfig.getOutputTypeArray()) {
            if (outputType.equals("kafka")) {
                appConfig.setOutputKafkaConf(appProperties.getProperty("app.output.kafka.conf"));
                appConfig.setOutputKafkaTopic(appProperties.getProperty("app.output.kafka.topic"));
            } else if (outputType.equals("elasticsearch")) {
                appConfig.setOutputElasticsearchHttpHostArray(appProperties.getProperty("app.output.elasticsearch.httpHosts").split(","));
                appConfig.setOutputElasticsearchHttpPort(Integer.valueOf(appProperties.getProperty("app.output.elasticsearch.httpPort")));
                appConfig.setOutputElasticsearchHttpScheme(appProperties.getProperty("app.output.elasticsearch.httpScheme"));
                appConfig.setOutputElasticsearchIndexPattern(appProperties.getProperty("app.output.elasticsearch.indexPattern"));
                appConfig.setOutputElasticsearchDocType(appProperties.getProperty("app.output.elasticsearch.docType"));
                appConfig.setOutputElasticsearchRequestTimeout(appProperties.getProperty("app.output.elasticsearch.requestTimeout"));
            }
        }

        return appConfig;
    }
}
