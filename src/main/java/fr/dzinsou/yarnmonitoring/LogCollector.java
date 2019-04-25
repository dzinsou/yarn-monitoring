package fr.dzinsou.yarnmonitoring;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import fr.dzinsou.yarnmonitoring.domain.Container;
import fr.dzinsou.yarnmonitoring.domain.LogEvent;
import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LogCollector {
    private final static Logger LOGGER = LoggerFactory.getLogger(LogCollector.class);

    private DumpContainerLog dumpContainerLog;

    private ObjectMapper objectMapper = new ObjectMapper();

    private LogCollector(UserGroupInformation ugi, Configuration conf) {
        this.dumpContainerLog = new DumpContainerLog(ugi, conf);
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("conf", "conf", true, "Configuration resource list");
        options.addOption("user", "user", true, "User");
        options.addOption("keytab", "keytab", true, "Keytab");
        options.addOption("mints", "mints", true, "Minimum creation time");
        options.addOption("maxts", "maxts", true, "Maximum creation time");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        String confResourceList = cmd.getOptionValue("conf");
        String user = cmd.getOptionValue("user");
        String keytab = cmd.getOptionValue("keytab");
        Long minStamp = Long.parseLong(cmd.getOptionValue("mints"));
        Long maxStamp = Long.parseLong(cmd.getOptionValue("maxts"));

        LOGGER.info("conf  : [{}]", confResourceList);
        LOGGER.info("user  : [{}]", user);
        LOGGER.info("keytab: [{}]", keytab);
        LOGGER.info("mints : [{}]", minStamp);
        LOGGER.info("maxts : [{}]", maxStamp);

        try {
            Configuration conf = HadoopUtil.getConfiguration(confResourceList);
            UserGroupInformation ugi = HadoopUtil.getUGI(conf, user, keytab);

            GetContainerList getContainerList = new GetContainerList(ugi, conf);
            LogCollector logCollector = new LogCollector(ugi, conf);

            for (Container container : getContainerList.getCompletedContainerList(minStamp, maxStamp)) {
                logCollector.processLogs(container);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    private void processLogs(Container container) throws IOException {
        LOGGER.info("{}", objectMapper.writeValueAsString(container));
        int dumpResult = dumpContainerLog.dumpAggregatedContainerLogs(container.getAppId(), container.getContainerId(), container.getContainerId());
        LOGGER.info("Dump logs result for container [{}]: [{}]", container.getContainerId(), dumpResult);
        if (dumpResult == 0) {
            this.readDumpedLogs(container);
        }
    }

    private void readDumpedLogs(Container container) throws IOException {
        File directory = new File(container.getContainerId());
        for (File file : FileUtils.listFiles(directory, null, true)) {
            if (file.isFile()) {
                LOGGER.info("File {}", file.getAbsolutePath());

                AtomicReference<String> logType = new AtomicReference<>();
                AtomicReference<String> logLastModifiedTime = new AtomicReference<>();
                AtomicReference<Long> logLength = new AtomicReference<>();
                AtomicBoolean isLogContent = new AtomicBoolean(false);
                AtomicReference<Long> logLine = new AtomicReference<>();

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
                        logEvent.setContainerInfo(container);
                        logEvent.setLogType(logType.get());
                        logEvent.setLogLastModifiedTime(logLastModifiedTime.get());
                        logEvent.setLogLength(logLength.get());
                        logEvent.setLogContent(line);
                        logEvent.setLogLine(logLine.get());
                        try {
                            LOGGER.info(objectMapper.writeValueAsString(logEvent));
                        } catch (JsonProcessingException e) {
                            LOGGER.error(e.getMessage());
                        }
                        logLine.set(logLine.get() + 1);
                    } else {
                        LOGGER.info(line);
                    }
                });
            }
        }
    }
}
