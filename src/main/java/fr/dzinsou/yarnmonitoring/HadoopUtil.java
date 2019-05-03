package fr.dzinsou.yarnmonitoring;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

class HadoopUtil {
    private final static Logger LOGGER = LoggerFactory.getLogger(HadoopUtil.class);

    static Configuration getConfiguration(String confResourceList) {
        Configuration conf = new Configuration();
        for (String confResource : confResourceList.split(",")) {
            LOGGER.info("Adding this configuration resource: [{}]", confResource);
            conf.addResource(new Path(confResource));
        }
        return conf;
    }

    static UserGroupInformation getUGI(Configuration conf, String user, String keytab) throws IOException {
        UserGroupInformation.setConfiguration(conf);
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user, keytab);
    }
}
