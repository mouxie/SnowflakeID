package io.github.mouxie.snowflake;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * @author https://yanmouxie.com
 */
public class Snowflake {

    private Logger logger = LoggerFactory.getLogger(Snowflake.class);
    private volatile static Snowflake instance;
    private long epoch;
    private final long sequenceBits = 8L;
    private final long maxSequence = ~(-1L << sequenceBits);
    private final long sequenceMaskBits = 4L;
    private final long maxSequenceMask = ~(-1L << sequenceMaskBits);
    private final long workerIdBits = 10L;
    private final long maxWorkerId = ~(-1L << workerIdBits);
    private final long sequenceMaskShift = sequenceBits;
    private final long workerIdShift = sequenceBits + sequenceMaskBits;
    private final long timestampLeftShift = sequenceBits + sequenceMaskBits + workerIdBits;
    private long sequence = 0L;
    private static long sequenceMask = 0L;
    private long workerId = 1L;
    private long lastTimestamp = -1L;
    private String applicationName;
    private final String sequenceMaskFilePath = System.getProperty("java.io.tmpdir") + File.separator + "sequenceMask";

    private Snowflake() {
    }

    public Snowflake(long epoch, String applicationName, long workerId) {

        if (timeGen() > epoch) {
            this.epoch = epoch;
        } else {
            throw new IllegalArgumentException("Snowflake not support epoch gt currentTime");
        }

        if (workerId >= 0 && workerId <= maxWorkerId) {
            this.workerId = workerId;
        } else {
            throw new IllegalArgumentException("workerID must gte 0 and lte 1023");
        }

        this.applicationName = applicationName;
        loadSequenceMask();

        logger.info("epoch-{}, workerId-{}, sequenceMask-{}", this.epoch, this.workerId, sequenceMask);
    }

    public Snowflake(long epoch, String applicationName, String zkConnectionString) {

        if (timeGen() > epoch) {
            this.epoch = epoch;
        } else {
            throw new IllegalArgumentException("Snowflake not support epoch gt currentTime");
        }

        SnowflakeZookeeperHolder holder = new SnowflakeZookeeperHolder(zkConnectionString, applicationName, maxWorkerId);
        boolean initFlag = holder.init();
        if (initFlag) {
            workerId = holder.getWorkerID();
            logger.info("Started snowflake ID service by using Zookeeper, workerId-{}", workerId);
        } else {
            throw new IllegalArgumentException("Failed to initialize SnowflakeZookeeperHolder.");
        }

        if (workerId < 0 || workerId > maxWorkerId) {
            throw new IllegalArgumentException("workerID must gte 0 and lte 1023.");
        }

        this.applicationName = applicationName;
        loadSequenceMask();

        logger.info("epoch-{}, zookeeper-{}, sequenceMask-{}", this.epoch, zkConnectionString, sequenceMask);
    }

    public static synchronized Snowflake getInstance(long epoch, String applicationName, String zkConnectionString) {

        if (instance == null) {
            synchronized (Snowflake.class) {
                if (instance == null) {
                    instance = new Snowflake(epoch, applicationName, zkConnectionString);
                }
            }
        }
        return instance;
    }

    public static synchronized Snowflake getInstance(long epoch, String applicationName, long workerId) {

        if (instance == null) {
            synchronized (Snowflake.class) {
                if (instance == null) {
                    instance = new Snowflake(epoch, applicationName, workerId);
                }
            }
        }
        return instance;
    }

    public synchronized long nextId() {
        long timestamp = timeGen();
        if (timestamp < lastTimestamp) {
            logger.info("Clock moved backwards.");
            long offset = lastTimestamp - timestamp;
            if (offset <= 5) {
                try {
                    wait(offset << 1);
                    timestamp = timeGen();
                    if (timestamp < lastTimestamp) {
                        return handleClockBackwards(timestamp);
                    }
                } catch (InterruptedException e) {
                    logger.error("wait interrupted");
                    return handleClockBackwards(timestamp);
                }
            } else {
                return handleClockBackwards(timestamp);
            }
        }
        if (lastTimestamp == timestamp) {
            sequence = (sequence + 1) & maxSequence;
            if (sequence == 0) {
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }
        lastTimestamp = timestamp;
        return ((timestamp - epoch) << timestampLeftShift) | (workerId << workerIdShift) | (sequenceMask << sequenceMaskShift) | sequence;
    }

    private long handleClockBackwards(long timestamp) {

        logger.info("Enter - handleClockBackwards()");

        sequenceMask = (sequenceMask + 1) & maxSequenceMask;

        setSequenceMask(sequenceMask);

        lastTimestamp = timestamp;

        return ((timestamp - epoch) << timestampLeftShift) | (workerId << workerIdShift) | (sequenceMask << sequenceMaskShift) | sequence;
    }

    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen();
        while (timestamp <= lastTimestamp) {
            timestamp = timeGen();
        }
        return timestamp;
    }

    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public void setWorkerId(long workerId) {
        this.workerId = workerId;
    }

    public long getWorkerId() {
        return workerId;
    }

    private void loadSequenceMask() {
        File sequenceMaskFile = new File(sequenceMaskFilePath);
        boolean exists = sequenceMaskFile.exists();
        if (exists) {
            Properties properties = new Properties();
            try {
                properties.load(new FileInputStream(new File(sequenceMaskFilePath)));
                sequenceMask = Integer.valueOf(properties.getProperty("sequenceMask"));
                logger.info("sequenceMask loaded :{}", sequenceMask);
            } catch (IOException e) {
                logger.warn("Failed to read sequenceMask from file", e);
            }
        }
    }

    private void setSequenceMask(long newSequenceMask) {

        File sequenceMaskFile = new File(sequenceMaskFilePath);

        boolean exists = sequenceMaskFile.exists();

        if (exists) {
            //update
            logger.info("update file " + sequenceMaskFilePath);
            try {
                FileUtils.writeStringToFile(sequenceMaskFile, "sequenceMask=" + newSequenceMask, false);
                logger.info("local file cache sequenceMask is {}", newSequenceMask);
            } catch (IOException e) {
                logger.error("Failed to update sequenceMaskFile", e);
            }
        } else {
            try {
                if (sequenceMaskFile.createNewFile()) {
                    FileUtils.writeStringToFile(sequenceMaskFile, "sequenceMask=" + newSequenceMask, false);
                    logger.info("local file cache sequenceMask is {}", newSequenceMask);
                }
            } catch (IOException e) {
                logger.error("Failed to create sequenceMaskFile", e);
            }
        }
    }
}
