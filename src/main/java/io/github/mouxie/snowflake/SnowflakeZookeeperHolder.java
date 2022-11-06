package io.github.mouxie.snowflake;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class SnowflakeZookeeperHolder {

    private Logger logger = LoggerFactory
            .getLogger(SnowflakeZookeeperHolder.class);

    private int workerID;

    private String zkAddressNode;
    private String nodeKeyName;

    private String prefixZkPath;
    private String pathForever;
    private String workIDFilePath;
    private String nodeKeyNameFilePath;

    private String connectionString;
    private long lastUpdateTime;

    public SnowflakeZookeeperHolder(String connectionString, String applicationName) {
        this.connectionString = connectionString;
        this.prefixZkPath = "/snowflake/" + applicationName;
        this.nodeKeyNameFilePath = System.getProperty("java.io.tmpdir") + File.separator + applicationName
                + File.separator + "nodeKeyName";
        setNodeKeyName();
        this.workIDFilePath = System.getProperty("java.io.tmpdir") + File.separator + applicationName
                + File.separator + this.nodeKeyName + File.separator + "workerID";
        this.pathForever = this.prefixZkPath + "/forever";
    }

    public boolean init() {
        try {
            CuratorFramework curator = createWithOptions(connectionString, new RetryUntilElapsed(1000, 4), 10000, 10000);
            curator.start();
            Stat stat = curator.checkExists().forPath(this.pathForever);
            if (stat == null) {
                zkAddressNode = createNode(curator);
                updateLocalWorkerID(workerID);
                return true;
            } else {
                Map<String, Integer> nodeMap = Maps.newHashMap();
                Map<String, String> realNode = Maps.newHashMap();

                List<String> keys = curator.getChildren().forPath(pathForever);
                for (String key : keys) {
                    String[] nodeKey = key.split("-----");
                    if (nodeKey.length == 2) {
                        realNode.put(nodeKey[0], key);
                        nodeMap.put(nodeKey[0], Integer.parseInt(nodeKey[1]));
                    }
                }

                Integer workerid = nodeMap.get(nodeKeyName);
                if (workerid != null) {
                    zkAddressNode = pathForever + "/" + realNode.get(nodeKeyName);
                    workerID = workerid;
                    updateLocalWorkerID(workerID);
                    logger.info("[Old NODE]forever node {} was found with workid-{} and started successfully", nodeKeyName, workerID);
                } else {
                    // new node
                    String newNode = createNode(curator);
                    zkAddressNode = newNode;
                    logger.info("created newNode:" + newNode);
                    String[] nodeKey = newNode.split("-----");
                    workerID = Integer.parseInt(nodeKey[1]);
                    updateLocalWorkerID(workerID);
                    logger.info("[New NODE]forever node was not found,create a new node[{nodeKeyName}] on forever node with wokerID-{workerID} and started successfully ", nodeKeyName, workerID);
                }
            }
        } catch (Exception e) {
            logger.error("Failed to start node, ERROR {}", e);
            try {
                File workIDFile = new File(this.workIDFilePath);
                boolean exists = workIDFile.exists();
                if (exists) {
                    Properties properties = new Properties();
                    properties.load(new FileInputStream(new File(workIDFilePath)));
                    workerID = Integer.valueOf(properties.getProperty("workerID"));
                    logger.warn("Failed to start with Zookeeper,use local node file properties workerID-{}", workerID);
                }
            } catch (Exception err) {
                logger.error("Failed to read workerID file ", err);
                return false;
            }
        }
        return true;
    }

    private void setNodeKeyName() {
        File nodeKeyNameFile = new File(this.nodeKeyNameFilePath);
        boolean exists = nodeKeyNameFile.exists();
        logger.info("nodeKeyNameFile exists status is {}", exists);
        if (exists) {
            try {
                Properties properties = new Properties();
                properties.load(new FileInputStream(new File(nodeKeyNameFilePath)));
                this.nodeKeyName = properties.getProperty("nodeKeyName");
                logger.info("nodeKeyName-{}", this.nodeKeyName);
            } catch (Exception e) {
                logger.error("Failed to read file nodeKeyName", e);
            }
        } else {
            try {
                boolean mkdirs = nodeKeyNameFile.getParentFile().mkdirs();
                logger.info("init local file cache create parent dir status is {}", mkdirs);
                if (mkdirs) {
                    if (nodeKeyNameFile.createNewFile()) {
                        this.nodeKeyName = String.valueOf(System.currentTimeMillis());
                        FileUtils.writeStringToFile(nodeKeyNameFile, "nodeKeyName=" + this.nodeKeyName, false);
                        logger.info("local file cache nodeKeyName is {}", this.nodeKeyName);
                    }
                } else {
                    logger.warn("Failed to create parent dir");
                }
            } catch (IOException e) {
                logger.warn("Failed to create nodeKeyName file error", e);
            }
        }
    }

    private String createNode(CuratorFramework curator) throws Exception {
        try {
            return curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).
                    forPath(pathForever + "/" + nodeKeyName + "-----", nodeKeyName.getBytes());
        } catch (Exception e) {
            logger.error("Failed to create node, error msg {} ", e.getMessage());
            throw e;
        }
    }

    private void updateLocalWorkerID(int workerID) {
        File workIDFile = new File(workIDFilePath);
        boolean exists = workIDFile.exists();
        logger.info("workIDFile exists status is {}", exists);
        if (exists) {
            try {
                FileUtils.writeStringToFile(workIDFile, "workerID=" + workerID, false);
                logger.info("update file cache workerID: {}", workerID);
            } catch (IOException e) {
                logger.error("Failed to update file cache workerID", e);
            }
        } else {
            try {
                boolean mkdirs = workIDFile.getParentFile().mkdirs();
                logger.info("Created parent directory:{}, workerId is {}", mkdirs, workerID);
                if (mkdirs) {
                    if (workIDFile.createNewFile()) {
                        FileUtils.writeStringToFile(workIDFile, "workerID=" + workerID, false);
                        logger.info("local file cache workerID: {}", workerID);
                    }
                } else {
                    logger.warn("Failed to create parent dir");
                }
            } catch (IOException e) {
                logger.warn("Failed to create workerID file", e);
            }
        }
    }

    private CuratorFramework createWithOptions(String connectionString,
                                               RetryPolicy retryPolicy,
                                               int connectionTimeoutMs,
                                               int sessionTimeoutMs) {
        CuratorFramework client =
                CuratorFrameworkFactory.newClient(connectionString, sessionTimeoutMs, connectionTimeoutMs, retryPolicy);
        return client;
    }

    public int getWorkerID() {
        return workerID;
    }
}
