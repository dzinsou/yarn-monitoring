package fr.dzinsou.yarnmonitoring;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.logaggregation.ContainerLogsRequest;
import org.apache.hadoop.yarn.logaggregation.LogCLIHelpers;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class DumpContainerLog {
    private final static Logger LOGGER = LoggerFactory.getLogger(DumpContainerLog.class);

    private UserGroupInformation ugi;
    private Configuration conf;

    DumpContainerLog(UserGroupInformation ugi, Configuration conf) {
        this.ugi = ugi;
        this.conf = conf;
    }

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("conf", "conf", true, "Configuration resource list");
        options.addOption("user", "user", true, "User");
        options.addOption("keytab", "keytab", true, "Keytab");
        options.addOption("app", "app", true, "Application ID");
        options.addOption("container", "container", true, "Container ID");
        options.addOption("output", "output", true, "Output directory");

        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(options, args);

        String confResourceList = cmd.getOptionValue("conf");
        String user = cmd.getOptionValue("user");
        String keytab = cmd.getOptionValue("keytab");
        String appIdStr = cmd.getOptionValue("app");
        String containerIdStr = cmd.getOptionValue("container");
        String outputLocalDir = cmd.getOptionValue("output");

        LOGGER.info("confResourceList: [{}]", confResourceList);
        LOGGER.info("user            : [{}]", user);
        LOGGER.info("keytab          : [{}]", keytab);
        LOGGER.info("appIdStr        : [{}]", appIdStr);
        LOGGER.info("containerIdStr  : [{}]", containerIdStr);
        LOGGER.info("outputLocalDir  : [{}]", outputLocalDir);

        try {
            Configuration conf = HadoopUtil.getConfiguration(confResourceList);
            UserGroupInformation ugi = HadoopUtil.getUGI(conf, user, keytab);
            DumpContainerLog dumpContainerLog = new DumpContainerLog(ugi, conf);
            LOGGER.info("Dump logs result: [{}]", dumpContainerLog.dumpAggregatedContainerLogs(appIdStr, containerIdStr, outputLocalDir));
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    int dumpAggregatedContainerLogs(String appIdStr, String containerIdStr, String outputLocalDir) throws IOException {
        this.ugi.checkTGTAndReloginFromKeytab();
        try {
            return this.ugi.doAs((PrivilegedExceptionAction<Integer>) () -> {
                LogCLIHelpers logCLIHelpers = new LogCLIHelpers();
                logCLIHelpers.setConf(conf);

                ApplicationId appId = ConverterUtils.toApplicationId(appIdStr);
                ContainerLogsRequest containerLogsRequest = new ContainerLogsRequest();
                containerLogsRequest.setAppId(appId);
                containerLogsRequest.setContainerId(containerIdStr);
                containerLogsRequest.setOutputLocalDir(outputLocalDir);
                containerLogsRequest.setBytes(Long.MAX_VALUE);

                return logCLIHelpers.dumpAllContainersLogs(containerLogsRequest);
            });
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

        return -2;
    }
}
