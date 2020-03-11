package com.dtwave.flume.sink;

import com.alibaba.fastjson.JSONObject;
import com.dtwave.common.http.Request;
import com.dtwave.common.http.Response;
import com.dtwave.common.util.RestUtil;
import com.dtwave.flume.dto.FlumeReportStatusDto;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Description: 杀死作业创建Killed文件 SINK
 *
 * @author hao.lh
 * @version 1.0
 * create date time: 2020/1/15 上午11:45.
 * update date time:
 */
public class FileSink extends AbstractSink implements Configurable {

    private static final Logger logger = LoggerFactory.getLogger(FileSink.class);

    private static final String SLASH = "/";
    private static final String DEFAULT_FLUME_TASK_LOG_DIR = "/opt/third/codes";

    private String baseDir;

    private List<String> masterUrls;

    private static final int defaultReportCoreSize = 5;

    /**
     * 定时汇报作业状态线程池
     */
    private ScheduledExecutorService timedReportPool;

    @Override
    public Status process() throws EventDeliveryException {
        Channel channel = getChannel();
        Transaction transaction = channel.getTransaction();
        transaction.begin();
        try {
            Event event = channel.take();
            if (null == event) {
                transaction.commit();
                return Status.READY;
            }
            logger.info("收到作业杀死标识符");
            Map<String, String> header = event.getHeaders();
            if (null != header) {
                String taskId = header.get("taskId");
                String instanceId = header.get("instanceId");
                String fullPath = baseDir + SLASH + instanceId + SLASH + taskId + ".killed";
                if (!Files.isDirectory(new File(baseDir).toPath())) {
                    try {
                        Files.createDirectory(new File(baseDir).toPath());
                    } catch (IOException e) {
                        logger.error("base dir not exits", e);
                        transaction.commit();
                        return Status.READY;
                    }
                }
                if (!Files.isDirectory(new File(baseDir + SLASH + instanceId).toPath())) {
                    try {
                        Files.createDirectory(new File(baseDir + SLASH + instanceId).toPath());
                    } catch (IOException e) {
                        logger.error("base dir instance {} not exits", instanceId, e);
                        transaction.commit();
                        return Status.READY;
                    }
                }
                try {
                    Files.createFile(new File(fullPath).toPath());
                    logger.info("作业杀死状态变更成功");
                    kill(taskId);
                } catch (IOException e) {
                    logger.error("base file {} create error", fullPath, e);
                    transaction.commit();
                    return Status.READY;
                }
            } else {
                logger.error("flume task header must not be null");
                transaction.commit();
                throw new EventDeliveryException();
            }
            transaction.commit();
            return Status.READY;
        } catch (EventDeliveryException e) {
            throw e;
        } finally {
            transaction.close();
        }
    }

    @Override
    public void configure(Context context) {
        baseDir = context.getString("task.log.baseDir", DEFAULT_FLUME_TASK_LOG_DIR);
        masterUrls = new ArrayList<>();
        Collections.addAll(masterUrls, context.getString("task.master.urls").split(","));
    }

    @Override
    public void start() {
        // 定时汇报作业状态线程池
        String reportName = "file-" + getName() + "-report-timer-%d";
        timedReportPool = new ScheduledThreadPoolExecutor(defaultReportCoreSize,
                new ThreadFactoryBuilder().setNameFormat(reportName).build());
    }

    /**
     * 汇报master 作业杀死状态
     *
     * @param taskId 作业ID
     */
    private void kill(String taskId) {
        String masterUrl = pickMaster(masterUrls);
        if (null != masterUrl) {
            int status = com.dtwave.dipper.dubhe.common.constant.Status.KILLED.getIndex();
            schedule(taskId, masterUrls, masterUrl, JSONObject.toJSONString(FlumeReportStatusDto.builder().taskId(taskId).statusIndex(status).build()), "/update_task");
        } else {
            logger.error("flume task report master url is null");
        }
    }

    /**
     * 汇报作业状态到master
     *
     * @param taskId     作业ID
     * @param masterUrls master 地址集合
     * @param masterUrl  当前汇报的master地址
     * @param body       参数body
     * @param path       参数路径
     */
    private void schedule(String taskId, List<String> masterUrls, String masterUrl, String body, String path) {
        Callable<Response> responseCallable = () -> new Request(masterUrl + path).body(body).POST();
        ScheduledFuture<Response> future = timedReportPool.schedule(responseCallable, 1L, TimeUnit.SECONDS);
        try {
            Response response = future.get(60000L, TimeUnit.SECONDS);
            if (!RestUtil.isSuccess(response)) {
                masterUrls.remove(0);
                String anotherMasterUrl = pickMaster(masterUrls);
                if (null != anotherMasterUrl) {
                    schedule(taskId, masterUrls, anotherMasterUrl, body, path);
                }
                logger.warn("flume task report master url {} no response", masterUrl);
            } else {
                logger.info("flume task {} report successfully", taskId);
            }
        } catch (InterruptedException e) {
            logger.error("flume task report master {} has been interrupted", masterUrl, e);
        } catch (ExecutionException e) {
            logger.error("flume task report master url {} execution error", masterUrl, e);
        } catch (TimeoutException e) {
            logger.error("flume task report master url {} timeout", masterUrl, e);
            String anotherMasterUrl = pickMaster(masterUrls);
            if (null != anotherMasterUrl) {
                schedule(taskId, masterUrls, anotherMasterUrl, body, path);
            }
        }
    }

    /**
     * 挑选master
     *
     * @param masterUrls master地址集合
     * @return masterUrl
     */
    private String pickMaster(List<String> masterUrls) {
        if (CollectionUtils.isNotEmpty(masterUrls)) {
            return masterUrls.get(0);
        }
        return null;
    }

}
