package fr.dzinsou.yarnmonitoring;

import com.fasterxml.jackson.databind.ObjectMapper;
import fr.dzinsou.yarnmonitoring.domain.Container;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

public class GetContainerList {
    private final static Logger LOGGER = LoggerFactory.getLogger(GetContainerList.class);

    private UserGroupInformation ugi;
    private Configuration conf;

    GetContainerList(UserGroupInformation ugi, Configuration conf) {
        this.ugi = ugi;
        this.conf = conf;
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
            ObjectMapper objectMapper = new ObjectMapper();
            Configuration conf = HadoopUtil.getConfiguration(confResourceList);
            UserGroupInformation ugi = HadoopUtil.getUGI(conf, user, keytab);
            GetContainerList getContainerList = new GetContainerList(ugi, conf);
            for (Container container : getContainerList.getCompletedContainerList(minStamp, maxStamp)) {
                LOGGER.info("{}", objectMapper.writeValueAsString(container));
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    List<Container> getCompletedContainerList(Long minStamp, Long maxStamp) throws IOException {
        List<Container> containerList = new ArrayList<>();

        this.ugi.checkTGTAndReloginFromKeytab();
        try {
            this.ugi.doAs((PrivilegedExceptionAction<Void>) () -> {
                try (YarnClient yarnClient = YarnClient.createYarnClient()) {
                    yarnClient.init(conf);
                    yarnClient.start();
                    // Get applications
                    for (ApplicationReport application : yarnClient.getApplications(EnumSet.of(YarnApplicationState.FINISHED, YarnApplicationState.KILLED, YarnApplicationState.FAILED))) {
                        // Get application attempts
                        for (ApplicationAttemptReport applicationAttempt : yarnClient.getApplicationAttempts(application.getApplicationId())) {
                            ApplicationAttemptId applicationAttemptId = applicationAttempt.getApplicationAttemptId();
                            // Get application attempt containers
                            List<ContainerReport> containerReportList = new ArrayList<>();
                            try {
                                containerReportList.addAll(yarnClient.getContainers(applicationAttemptId));
                            } catch (ApplicationNotFoundException e) {
                                LOGGER.debug("Application attempt not found: [{}]", applicationAttemptId.toString());
                            }

                            for (ContainerReport containerReport : containerReportList) {
                                Long containerCreationTime = containerReport.getCreationTime();
                                if (containerCreationTime >= minStamp && containerCreationTime <= maxStamp) {
                                    Container container = new Container();
                                    container.setContainerId(containerReport.getContainerId().toString());
                                    container.setAppId(application.getApplicationId().toString());
                                    container.setAppName(application.getName());
                                    container.setAppType(application.getApplicationType());
                                    container.setAppState(application.getYarnApplicationState().name());
                                    container.setAppFinalStatus(application.getFinalApplicationStatus().name());
                                    if (application.getLogAggregationStatus() != null) {
                                        String logAggStatus = application.getLogAggregationStatus().name();
                                        container.setAppLogAggStatus(logAggStatus);
                                    }
                                    container.setAppAttemptId(applicationAttemptId.toString());
                                    container.setQueue(application.getQueue());
                                    container.setUser(application.getUser());
                                    container.setAssignedNodeId(containerReport.getAssignedNode().toString());
                                    container.setAssignedNodeHttpAddress(containerReport.getNodeHttpAddress());
                                    container.setAllocatedMemorySize(containerReport.getAllocatedResource().getMemorySize());
                                    container.setAllocatedVirtualCores(containerReport.getAllocatedResource().getVirtualCores());
                                    container.setContainerExitStatus(containerReport.getContainerExitStatus());
                                    if (containerReport.getContainerState() != null) {
                                        container.setContainerState(containerReport.getContainerState().name());
                                    }
                                    container.setCreationTime(containerReport.getCreationTime());
                                    container.setFinishTime(containerReport.getFinishTime());
                                    container.setLogUrl(containerReport.getLogUrl());
                                    container.setPriority(containerReport.getPriority().getPriority());
                                    containerList.add(container);
                                }
                            }
                        }
                    }
                }
                return null;
            });
        } catch (InterruptedException e) {
            LOGGER.error(e.getMessage(), e);
        }

        return containerList;
    }
}
