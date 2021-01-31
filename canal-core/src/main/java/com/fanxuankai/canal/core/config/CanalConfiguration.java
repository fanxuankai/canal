package com.fanxuankai.canal.core.config;

import org.springframework.util.StringUtils;

/**
 * canal参数配置
 *
 * @author fanxuankai
 */
public class CanalConfiguration {

    /**
     * 应用 id, 用于 canal client 抢占式运行、binlog offset 防重, 默认取 instance
     */
    private String id;

    /**
     * 集群配置
     */
    private Cluster cluster;

    /**
     * 单节点配置
     */
    private SingleNode singleNode = new SingleNode();

    /**
     * 实例
     */
    private String instance = "example";

    /**
     * 过滤
     */
    private String filter = ".*\\..*";

    /**
     * 账号
     */
    private String username = "canal";

    /**
     * 密码
     */
    private String password = "canal";

    /**
     * 拉取数据的间隔 ms
     */
    private long intervalMillis = 1_000;

    /**
     * 拉取数据的数量
     */
    private int batchSize = 100;

    /**
     * 打印事件日志
     */
    private boolean showEventLog = true;

    /**
     * 打印 Entry 日志
     */
    private boolean showEntryLog = true;

    /**
     * 打印数据明细日志
     */
    private boolean showRowChange;

    /**
     * 格式化数据明细日志
     */
    private boolean formatRowChangeLog;

    /**
     * 批次达到一定数量进行并行处理, 且确保顺序消费
     */
    private int performanceThreshold = 10_000;

    /**
     * 跳过处理
     */
    private boolean skip;

    /**
     * 全局逻辑删除字段
     */
    private String logicDeleteField = "deleted";

    /**
     * 激活逻辑删除
     */
    private boolean enableLogicDelete;

    /**
     * 抢占式运行参数
     */
    private Preemptive preemptive = new Preemptive();

    /**
     * 并行处理
     */
    private Boolean parallel;

    /**
     * 相同 schema、table、eventType, 合并为一个 Entry
     */
    private MergeEntry mergeEntry = new MergeEntry();

    public String getId() {
        return StringUtils.hasText(id) ? id : instance;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public SingleNode getSingleNode() {
        return singleNode;
    }

    public void setSingleNode(SingleNode singleNode) {
        this.singleNode = singleNode;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public String getFilter() {
        return filter;
    }

    public void setFilter(String filter) {
        this.filter = filter;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public long getIntervalMillis() {
        return intervalMillis;
    }

    public void setIntervalMillis(long intervalMillis) {
        this.intervalMillis = intervalMillis;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public boolean isShowEventLog() {
        return showEventLog;
    }

    public void setShowEventLog(boolean showEventLog) {
        this.showEventLog = showEventLog;
    }

    public boolean isShowEntryLog() {
        return showEntryLog;
    }

    public void setShowEntryLog(boolean showEntryLog) {
        this.showEntryLog = showEntryLog;
    }

    public boolean isShowRowChange() {
        return showRowChange;
    }

    public void setShowRowChange(boolean showRowChange) {
        this.showRowChange = showRowChange;
    }

    public boolean isFormatRowChangeLog() {
        return formatRowChangeLog;
    }

    public void setFormatRowChangeLog(boolean formatRowChangeLog) {
        this.formatRowChangeLog = formatRowChangeLog;
    }

    public int getPerformanceThreshold() {
        return performanceThreshold;
    }

    public void setPerformanceThreshold(int performanceThreshold) {
        this.performanceThreshold = performanceThreshold;
    }

    public boolean isSkip() {
        return skip;
    }

    public void setSkip(boolean skip) {
        this.skip = skip;
    }

    public String getLogicDeleteField() {
        return logicDeleteField;
    }

    public void setLogicDeleteField(String logicDeleteField) {
        this.logicDeleteField = logicDeleteField;
    }

    public boolean isEnableLogicDelete() {
        return enableLogicDelete;
    }

    public void setEnableLogicDelete(boolean enableLogicDelete) {
        this.enableLogicDelete = enableLogicDelete;
    }

    public Preemptive getPreemptive() {
        return preemptive;
    }

    public void setPreemptive(Preemptive preemptive) {
        this.preemptive = preemptive;
    }

    public Boolean getParallel() {
        return parallel;
    }

    public void setParallel(Boolean parallel) {
        this.parallel = parallel;
    }

    public MergeEntry getMergeEntry() {
        return mergeEntry;
    }

    public void setMergeEntry(MergeEntry mergeEntry) {
        this.mergeEntry = mergeEntry;
    }

    public static class Cluster {
        /**
         * zookeeper host:port
         */
        private String nodes = "localhost:2181,localhost:2182,localhost:2183";

        public String getNodes() {
            return nodes;
        }

        public void setNodes(String nodes) {
            this.nodes = nodes;
        }
    }

    public static class SingleNode {
        /**
         * host
         */
        private String hostname = "localhost";
        /**
         * port
         */
        private int port = 11111;

        public String getHostname() {
            return hostname;
        }

        public void setHostname(String hostname) {
            this.hostname = hostname;
        }

        public int getPort() {
            return port;
        }

        public void setPort(int port) {
            this.port = port;
        }
    }

    public static class Preemptive {
        /**
         * 心跳超时 s
         */
        private Long timeout = 15L;
        /**
         * 心跳频率 s
         */
        private Long keep = 5L;
        /**
         * ping 的频率 s
         */
        private Long ping = 30L;

        public Long getTimeout() {
            return timeout;
        }

        public void setTimeout(Long timeout) {
            this.timeout = timeout;
        }

        public Long getKeep() {
            return keep;
        }

        public void setKeep(Long keep) {
            this.keep = keep;
        }

        public Long getPing() {
            return ping;
        }

        public void setPing(Long ping) {
            this.ping = ping;
        }
    }

    public static class MergeEntry {
        /**
         * 是否合并
         */
        private Boolean merge;

        /**
         * 合并后的最大数据行数量
         */
        private Integer maxRowDataSize = 1000;

        public Boolean getMerge() {
            return merge;
        }

        public void setMerge(Boolean merge) {
            this.merge = merge;
        }

        public Integer getMaxRowDataSize() {
            return maxRowDataSize;
        }

        public void setMaxRowDataSize(Integer maxRowDataSize) {
            this.maxRowDataSize = maxRowDataSize;
        }
    }

}
