package fr.dzinsou.yarnmonitoring;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.dzinsou.yarnmonitoring.domain.Container;
import fr.dzinsou.yarnmonitoring.domain.LogEvent;
import fr.dzinsou.yarnmonitoring.output.AbstractOutput;
import fr.dzinsou.yarnmonitoring.output.KafkaOutput;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.shaded.org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class LogCollector {
    private final static Logger LOGGER = LoggerFactory.getLogger(LogCollector.class);

    private GetContainerList getContainerList;

    private DumpContainerLog dumpContainerLog;

    private ObjectMapper objectMapper = new ObjectMapper();

    private AppConfig appConfig;

    private LogCollector(AppConfig appConfig) throws IOException {
        this.appConfig = appConfig;
        Configuration conf = HadoopUtil.getConfiguration(appConfig.getHadoopConf());
        UserGroupInformation ugi = HadoopUtil.getUGI(conf, appConfig.getHadoopUser(), appConfig.getHadoopKeytab());
        this.getContainerList = new GetContainerList(ugi, conf);
        this.dumpContainerLog = new DumpContainerLog(ugi, conf);
    }

    public static void main(String[] args) throws ParseException, IOException {
        Options options = new Options();
        options.addOption("conf", "conf", true, "Application configuration");
        options.addOption("mints", "mints", true, "Minimum creation time");
        options.addOption("maxts", "maxts", true, "Maximum creation time");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        String appConfigFile = cmd.getOptionValue("conf");
        Long minStamp = Long.parseLong(cmd.getOptionValue("mints"));
        Long maxStamp = Long.parseLong(cmd.getOptionValue("maxts"));

        AppConfig appConfig = AppUtil.loadAppConfig(appConfigFile);

        LOGGER.info("LogCollector - FROM [{}] TO [{}]", minStamp, maxStamp);

        List<AbstractOutput> outputList = new ArrayList<>();
        for (String outputType : appConfig.getOutputTypeArray()) {
            if (outputType.equals("kafka")) {
                KafkaOutput kafkaOutput = new KafkaOutput(appConfig);
                outputList.add(kafkaOutput);
            }
        }

        try {
            LogCollector logCollector = new LogCollector(appConfig);

            LOGGER.info("Getting completed container list");
            List<Container> containerList = logCollector.getGetContainerList().getCompletedContainerList(minStamp, maxStamp);
            LOGGER.info("Found [{}] completed containers", containerList.size());

            for (Container container : containerList) {
                logCollector.processLogs(container, outputList);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            for (AbstractOutput output : outputList) {
                output.close();
            }
        }
    }

    private void processLogs(Container container, List<AbstractOutput> outputList) throws IOException {
        LOGGER.info("Processing logs of container: [{}]", objectMapper.writeValueAsString(container));
        int dumpResult = dumpContainerLog.dumpAggregatedContainerLogs(container.getAppId(), container.getContainerId(), container.getContainerId());
        LOGGER.info("Dump logs result for container [{}]: [{}]", container.getContainerId(), dumpResult);
        if (dumpResult == 0) {
            this.readDumpedLogs(container, outputList);
        }
    }

    private void readDumpedLogs(Container container, List<AbstractOutput> outputList) throws IOException {
        File directory = new File(container.getContainerId());
        for (File file : FileUtils.listFiles(directory, null, true)) {
            if (file.isFile()) {
                LOGGER.info("File {}", file.getAbsolutePath());

                AtomicReference<String> logType = new AtomicReference<>();
                AtomicReference<String> logLastModifiedTime = new AtomicReference<>();
                AtomicReference<Long> logLength = new AtomicReference<>();
                AtomicBoolean isLogContent = new AtomicBoolean(false);
                AtomicReference<Long> logLine = new AtomicReference<>();

                List<LogEvent> bulkRecordList = new ArrayList<>();
                AtomicLong totalSent = new AtomicLong(0L);

                Files.lines(Paths.get(file.getAbsolutePath())).forEach(line -> {
                    if (line.startsWith("LogType:")) {
                        logType.set(line.substring(8));
                        LOGGER.info("logType: [{}]", logType.get());
                    } else if (line.startsWith("LogLastModifiedTime:")) {
                        logLastModifiedTime.set(line.substring(20));
                        LOGGER.info("logLastModifiedTime: [{}]", logLastModifiedTime.get());
                    } else if (line.startsWith("LogLength:")) {
                        logLength.set(Long.parseLong(line.substring(10)));
                        LOGGER.info("logLength: [{}]", logLength.get());
                    } else if (line.startsWith("LogContents:")) {
                        isLogContent.set(true);
                        logLine.set(0L);
                    } else if (line.startsWith("End of LogType:")) {
                        isLogContent.set(false);
                    } else if (isLogContent.get()) {
                        LogEvent logEvent = new LogEvent();
                        logEvent.setCluster(appConfig.getHadoopCluster());
                        logEvent.setContainerInfo(container);
                        logEvent.setLogType(logType.get());
                        logEvent.setLogLastModifiedTime(logLastModifiedTime.get());
                        logEvent.setLogLength(logLength.get());
                        logEvent.setLogContent(line);
                        logEvent.setLogLine(logLine.get());
                        String id = String.format("%s|%s|%d", logEvent.getContainerInfo().getContainerId(), logEvent.getLogType(), logEvent.getLogLine());
                        logEvent.setId(id);
                        bulkRecordList.add(logEvent);
                        logLine.set(logLine.get() + 1);

                        // Save to output
                        if (bulkRecordList.size() % this.appConfig.getOutputBatchSize() == 0) {
                            LOGGER.info("[{}] Pushing [{}] records to output - Total records => [{}]", container.getContainerId(), bulkRecordList.size(), totalSent.get() + bulkRecordList.size());
                            this.bulkSaveLogEvent(bulkRecordList, outputList);
                            totalSent.addAndGet(bulkRecordList.size());
                            bulkRecordList.clear();
                            try {
                                LOGGER.debug("Pause for {}ms", this.appConfig.getOutputBatchInterval());
                                Thread.sleep(this.appConfig.getOutputBatchInterval());
                            } catch (InterruptedException e) {
                                LOGGER.error(e.getMessage(), e);
                            }
                        }
                    } else {
                        LOGGER.info(line);
                    }
                });

                if (bulkRecordList.size() > 0) {
                    LOGGER.info("[{}] Pushing [{}] records to output - Total records => [{}]", container.getContainerId(), bulkRecordList.size(), totalSent.get() + bulkRecordList.size());
                    totalSent.addAndGet(bulkRecordList.size());
                    this.bulkSaveLogEvent(bulkRecordList, outputList);
                }
            }
        }
    }

    private void bulkSaveLogEvent(List<LogEvent> bulkRecordList, List<AbstractOutput> outputList) {
        try {
            List<String> idList = new ArrayList<>();
            List<String> partitionIdList = new ArrayList<>();
            List<String> jsonRecordList = new ArrayList<>();
            for (LogEvent logEvent : bulkRecordList) {
                idList.add(logEvent.getId());
                partitionIdList.add(logEvent.getContainerInfo().getContainerId());
                jsonRecordList.add(objectMapper.writeValueAsString(logEvent));
            }

            for (AbstractOutput output : outputList) {
                output.bulkSave(idList, partitionIdList, jsonRecordList, this.appConfig.getOutputAsync());
            }
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
        }
    }

    private GetContainerList getGetContainerList() {
        return getContainerList;
    }
}
