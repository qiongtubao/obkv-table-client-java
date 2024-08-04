/*-
 * #%L
 * OBKV Table Client Framework
 * %%
 * Copyright (C) 2021 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package com.alipay.oceanbase.rpc.table;

import com.alipay.oceanbase.rpc.ObTableClient;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.location.model.ObServerRoute;
import com.alipay.oceanbase.rpc.location.model.partition.ObPair;
import com.alipay.oceanbase.rpc.mutation.result.*;
import com.alipay.oceanbase.rpc.protocol.payload.ResultCodes;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObObj;
import com.alipay.oceanbase.rpc.protocol.payload.impl.ObRowKey;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.*;
import com.alipay.oceanbase.rpc.threadlocal.ThreadLocalMap;
import com.alipay.oceanbase.rpc.util.MonitorUtil;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.alipay.oceanbase.rpc.util.TableClientLoggerFactory.*;
import static com.alipay.oceanbase.rpc.util.TraceUtil.formatTraceMessage;

public class ObTableClientBatchOpsImpl extends AbstractTableBatchOps {

    private static final Logger   logger                  = TableClientLoggerFactory
                                                              .getLogger(ObTableClientBatchOpsImpl.class);

    private final ObTableClient   obTableClient;

    private ExecutorService       executorService;
    private boolean               returningAffectedEntity = false;
    private ObTableBatchOperation batchOperation;

    /*
     * Ob table client batch ops impl.
     */
    public ObTableClientBatchOpsImpl(String tableName, ObTableClient obTableClient) {
        this.tableName = tableName;
        this.obTableClient = obTableClient;
        this.batchOperation = new ObTableBatchOperation();
    }

    /*
     * Ob table client batch ops impl.
     */
    public ObTableClientBatchOpsImpl(String tableName, ObTableBatchOperation batchOperation,
                                     ObTableClient obTableClient) {
        this.tableName = tableName;
        this.obTableClient = obTableClient;
        this.batchOperation = batchOperation;
    }

    /*
     * Get ob table batch operation.
     */
    @Override
    public ObTableBatchOperation getObTableBatchOperation() {
        return batchOperation;
    }

    /*
     * Get.
     */
    @Override
    public void get(Object[] rowkeys, String[] columns) {
        addObTableClientOperation(ObTableOperationType.GET, rowkeys, columns, null);
    }

    /*
     * Update.
     */
    @Override
    public void update(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.UPDATE, rowkeys, columns, values);
    }

    /*
     * Delete.
     */
    @Override
    public void delete(Object[] rowkeys) {
        addObTableClientOperation(ObTableOperationType.DEL, rowkeys, null, null);
    }

    /*
     * Insert.
     */
    @Override
    public void insert(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.INSERT, rowkeys, columns, values);
    }

    /*
     * Replace.
     */
    @Override
    public void replace(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.REPLACE, rowkeys, columns, values);
    }

    /*
     * Insert or update.
     */
    @Override
    public void insertOrUpdate(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.INSERT_OR_UPDATE, rowkeys, columns, values);
    }

    /*
     * Increment.
     */
    @Override
    public void increment(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        returningAffectedEntity = withResult;
        addObTableClientOperation(ObTableOperationType.INCREMENT, rowkeys, columns, values);
    }

    /*
     * Append.
     */
    @Override
    public void append(Object[] rowkeys, String[] columns, Object[] values, boolean withResult) {
        returningAffectedEntity = withResult;
        addObTableClientOperation(ObTableOperationType.APPEND, rowkeys, columns, values);
    }

    /*
     * Put.
     */
    @Override
    public void put(Object[] rowkeys, String[] columns, Object[] values) {
        addObTableClientOperation(ObTableOperationType.PUT, rowkeys, columns, values);
    }

    private void addObTableClientOperation(ObTableOperationType type, Object[] rowkeys,
                                           String[] columns, Object[] values) {
        ObTableOperation instance = ObTableOperation.getInstance(type, rowkeys, columns, values);
        batchOperation.addTableOperation((instance));
    }

    /*
     * Execute.
     */
    public List<Object> execute() throws Exception {
        // consistent can not be sure
        List<Object> results = new ArrayList<Object>(batchOperation.getTableOperations().size());
        for (ObTableOperationResult result : executeInternal().getResults()) {
            int errCode = result.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                switch (result.getOperationType()) {
                    case GET:
                    case INCREMENT:
                    case APPEND:
                        results.add(result.getEntity().getSimpleProperties());
                        break;
                    default:
                        results.add(result.getAffectedRows());
                }
            } else {
                results.add(ExceptionUtil.convertToObTableException(result.getExecuteHost(),
                    result.getExecutePort(), result.getSequence(), result.getUniqueId(), errCode,
                    result.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    /*
     * Execute with result
     */
    public List<Object> executeWithResult() throws Exception {
        // consistent can not be sure
        List<Object> results = new ArrayList<Object>(batchOperation.getTableOperations().size());
        for (ObTableOperationResult result : executeInternal().getResults()) {
            int errCode = result.getHeader().getErrno();
            if (errCode == ResultCodes.OB_SUCCESS.errorCode) {
                switch (result.getOperationType()) {
                    case GET:
                    case INSERT:
                    case DEL:
                    case UPDATE:
                    case INSERT_OR_UPDATE:
                    case REPLACE:
                    case INCREMENT:
                    case APPEND:
                    case PUT:
                        results.add(new MutationResult(result));
                        break;
                    default:
                        throw new ObTableException("unknown operation type "
                                                   + result.getOperationType());
                }
            } else {
                results.add(ExceptionUtil.convertToObTableException(result.getExecuteHost(),
                    result.getExecutePort(), result.getSequence(), result.getUniqueId(), errCode,
                    result.getHeader().getErrMsg()));
            }
        }
        return results;
    }

    public Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitionPrepare()
                                                                                                      throws Exception {
        // consistent can not be sure
        //获得操作列表 
        List<ObTableOperation> operations = batchOperation.getTableOperations();
        //创建分区和对应信息的map
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitionOperationsMap = new HashMap<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>>();

        if (obTableClient.isOdpMode()) {
            ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>> obTableOperations = new ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>(
                new ObTableParam(obTableClient.getOdpTable()),
                new ArrayList<ObPair<Integer, ObTableOperation>>());
            for (int i = 0; i < operations.size(); i++) {
                ObTableOperation operation = operations.get(i);
                obTableOperations.getRight().add(
                    new ObPair<Integer, ObTableOperation>(i, operation));
            }
            partitionOperationsMap.put(0L, obTableOperations);
            return partitionOperationsMap;
        }
        //遍历操作获得rowkey
        for (int i = 0; i < operations.size(); i++) {
            ObTableOperation operation = operations.get(i);
            ObRowKey rowKeyObject = operation.getEntity().getRowKey();
            //rowkey obobj 长度
            int rowKeySize = rowKeyObject.getObjs().size();
            Object[] rowKey = new Object[rowKeySize];
            for (int j = 0; j < rowKeySize; j++) {
                rowKey[j] = rowKeyObject.getObj(j).getValue();
            }

            ObPair<Long, ObTableParam> tableObPair = obTableClient.getTable(tableName, rowKey,
                false, false, obTableClient.getRoute(batchOperation.isReadOnly()));
            ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>> obTableOperations = partitionOperationsMap
                .get(tableObPair.getLeft());
            if (obTableOperations == null) {
                obTableOperations = new ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>(
                    tableObPair.getRight(), new ArrayList<ObPair<Integer, ObTableOperation>>());
                partitionOperationsMap.put(tableObPair.getLeft(), obTableOperations);
            }
            obTableOperations.getRight().add(new ObPair<Integer, ObTableOperation>(i, operation));
        }

        return partitionOperationsMap;
    }

    /*
     * Partition execute.
     */
    public void partitionExecute(ObTableOperationResult[] results,
                                 Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitionOperation)
                                                                                                                                   throws Exception {
        ObTableParam tableParam = partitionOperation.getValue().getLeft();
        long tableId = tableParam.getTableId();//表Id
        long partId = tableParam.getPartitionId();//分区Id
        long originPartId = tableParam.getPartId(); //原始分区Id
        ObTable subObTable = tableParam.getObTable();//子表
        List<ObPair<Integer, ObTableOperation>> subOperationWithIndexList = partitionOperation
            .getValue().getRight();//获得与索引关联的操作列表

        ObTableBatchOperationRequest subRequest = new ObTableBatchOperationRequest(); //创建子请求对象
        ObTableBatchOperation subOperations = new ObTableBatchOperation();
        for (ObPair<Integer, ObTableOperation> operationWithIndex : subOperationWithIndexList) { //遍历subOperationWithIndexList，将每个 ObTableOperation 添加到 subOperations 中。
            subOperations.addTableOperation(operationWithIndex.getRight());
        }
        subOperations.setSameType(batchOperation.isSameType());
        subOperations.setReadOnly(batchOperation.isReadOnly());
        subOperations.setSamePropertiesNames(batchOperation.isSamePropertiesNames());
        subRequest.setBatchOperation(subOperations); //设置子操作属性一致
        subRequest.setTableName(tableName);//设置表名
        subRequest.setReturningAffectedEntity(returningAffectedEntity);// 设置返回受影响实体
        subRequest.setReturningAffectedRows(true);// 设置返回受影响行数
        subRequest.setTableId(tableId);// 设置表ID
        subRequest.setPartitionId(partId);// 设置分区ID
        subRequest.setEntityType(entityType);// 设置实体类型
        subRequest.setTimeout(subObTable.getObTableOperationTimeout());// 设置超时时间
        if (batchOperation.isReadOnly()) {
            subRequest.setConsistencyLevel(obTableClient.getReadConsistency()
                .toObTableConsistencyLevel()); //设置一致性级别
        }
        subRequest.setBatchOperationAsAtomic(isAtomicOperation()); //设置原子性
        subRequest.setBatchOpReturnOneResult(isReturnOneResult()); //设置是否返回单一结果
        ObTableBatchOperationResult subObTableBatchOperationResult; //初始化子操作结果

        boolean needRefreshTableEntry = false;//初始化是否需要刷新表条目
        int tryTimes = 0;//初始化尝试次数
        long startExecute = System.currentTimeMillis();//初始化执行开始时间
        Set<String> failedServerList = null;//初始化失败服务器列表
        ObServerRoute route = null;//初始化路由信息

        while (true) {
            obTableClient.checkStatus();//检查客户端的状态
            long currentExecute = System.currentTimeMillis();//计算已用时间
            long costMillis = currentExecute - startExecute;
            if (costMillis > obTableClient.getRuntimeMaxWait()) {//检查是否超时
                logger.error(
                    "tablename:{} partition id:{} it has tried " + tryTimes
                            + " times and it has waited " + costMillis
                            + "/ms which exceeds response timeout "
                            + obTableClient.getRuntimeMaxWait() + "/ms", tableName, partId);
                throw new ObTableTimeoutExcetion("it has tried " + tryTimes
                                                 + " times and it has waited " + costMillis
                                                 + "/ms which exceeds response timeout "
                                                 + obTableClient.getRuntimeMaxWait() + "/ms");
            }
            tryTimes++;//增加尝试次数
            try {
                if (obTableClient.isOdpMode()) {
                    subObTable = obTableClient.getOdpTable();
                } else {
                    // getTable() when we need retry
                    // we should use partIdx to get table
                    if (tryTimes > 1) {//重试
                        if (route == null) {//获得路由信息
                            route = obTableClient.getRoute(batchOperation.isReadOnly());
                        }
                        if (failedServerList != null) {// 如果有失败的服务器列表 (failedServerList)，则更新路由信息中的黑名单 调用 obTableClient.getTable() 来获取表。
                            route.setBlackList(failedServerList);
                        }
                        subObTable = obTableClient
                            .getTable(tableName, originPartId, needRefreshTableEntry,
                                obTableClient.isTableEntryRefreshIntervalWait(), route).getRight()
                            .getObTable();
                    }
                }
                subObTableBatchOperationResult = (ObTableBatchOperationResult) subObTable
                    .execute(subRequest);// 来执行子请求，并将结果赋值给 subObTableBatchOperationResult。
                obTableClient.resetExecuteContinuousFailureCount(tableName); // 重置连续失败计数。
                break;
            } catch (Exception ex) {
                if (obTableClient.isOdpMode()) {
                    if ((tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger
                            .warn(
                                "batch ops execute while meet Exception, tablename:{}, errorCode: {} , errorMsg: {}, try times {}",
                                tableName, ((ObTableException) ex).getErrorCode(), ex.getMessage(),
                                tryTimes);
                    } else {
                        throw ex;
                    }
                } else if (ex instanceof ObTableReplicaNotReadableException) {
                    if ((tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger.warn(
                            "tablename:{} partition id:{} retry when replica not readable: {}",
                            tableName, partId, ex.getMessage());
                        if (failedServerList == null) {
                            failedServerList = new HashSet<String>(); //初始化错误
                        }
                        failedServerList.add(subObTable.getIp()); //追加错误ip
                    } else {
                        logger.warn("exhaust retry when replica not readable: {}", ex.getMessage());
                        throw ex;
                    }
                } else if (ex instanceof ObTableException
                           && ((ObTableException) ex).isNeedRefreshTableEntry()) {
                    needRefreshTableEntry = true;
                    logger
                        .warn(
                            "tablename:{} partition id:{} batch ops refresh table while meet ObTableMasterChangeException, errorCode: {}",
                            tableName, partId, ((ObTableException) ex).getErrorCode(), ex);
                    if (obTableClient.isRetryOnChangeMasterTimes()
                        && (tryTimes - 1) < obTableClient.getRuntimeRetryTimes()) {
                        logger
                            .warn(
                                "tablename:{} partition id:{} batch ops retry while meet ObTableMasterChangeException, errorCode: {} , retry times {}",
                                tableName, partId, ((ObTableException) ex).getErrorCode(),
                                tryTimes, ex);
                    } else {
                        obTableClient.calculateContinuousFailure(tableName, ex.getMessage());
                        throw ex;
                    }
                } else {
                    obTableClient.calculateContinuousFailure(tableName, ex.getMessage());
                    throw ex;
                }
            }
            Thread.sleep(obTableClient.getRuntimeRetryInterval());//等一等重试
        }

        long endExecute = System.currentTimeMillis();//结束时间

        if (subObTableBatchOperationResult == null) { //如果 subObTableBatchOperationResult 为 null，则记录错误日志并抛出 ObTableUnexpectedException。
            RUNTIME
                .error(
                    "tablename:{} partition id:{} check batch operation result error: client get unexpected NULL result",
                    tableName, partId);
            throw new ObTableUnexpectedException(
                "check batch operation result error: client get unexpected NULL result");
        }

        List<ObTableOperationResult> subObTableOperationResults = subObTableBatchOperationResult
            .getResults();//获得子请求结果列表

        if (returnOneResult) { //只要一个结果
            ObTableOperationResult subObTableOperationResult = subObTableOperationResults.get(0);
            if (results[0] == null) {
                results[0] = new ObTableOperationResult();
                subObTableOperationResult.setExecuteHost(subObTable.getIp());
                subObTableOperationResult.setExecutePort(subObTable.getPort());
                results[0] = subObTableOperationResult;
            } else {
                results[0].setAffectedRows(results[0].getAffectedRows()
                                           + subObTableOperationResult.getAffectedRows());
            }
        } else {
            if (subObTableOperationResults.size() < subOperations.getTableOperations().size()) {//检查 subObTableOperationResults 的大小是否小于 subOperations.getTableOperations().size()。
                // only one result when it across failed
                // only one result when hkv puts
                if (subObTableOperationResults.size() == 1) {//如果是，且结果列表大小为 1，则设置执行主机和端口信息，并将这个结果复制到所有位置。
                    ObTableOperationResult subObTableOperationResult = subObTableOperationResults
                        .get(0);
                    subObTableOperationResult.setExecuteHost(subObTable.getIp());
                    subObTableOperationResult.setExecutePort(subObTable.getPort());
                    for (ObPair<Integer, ObTableOperation> aSubOperationWithIndexList : subOperationWithIndexList) {
                        results[aSubOperationWithIndexList.getLeft()] = subObTableOperationResult;
                    }
                } else {//如果不是，抛出 IllegalArgumentException。
                    // unexpected result found
                    throw new IllegalArgumentException(
                        "check batch operation result size error: operation size ["
                                + subOperations.getTableOperations().size() + "] result size ["
                                + subObTableOperationResults.size() + "]");
                }
            } else {//否则，验证 subOperationWithIndexList 和 subObTableOperationResults 的大小是否相等。
                if (subOperationWithIndexList.size() != subObTableOperationResults.size()) { //如果不相等，抛出 ObTableUnexpectedException。
                    throw new ObTableUnexpectedException("check batch result error: partition "
                                                         + partId + " expect result size "
                                                         + subOperationWithIndexList.size()
                                                         + " actual result size "
                                                         + subObTableOperationResults.size());
                }
                for (int i = 0; i < subOperationWithIndexList.size(); i++) {//遍历两个列表，设置每个结果的执行主机和端口信息，并将结果存储到相应的索引位置。
                    ObTableOperationResult subObTableOperationResult = subObTableOperationResults
                        .get(i);
                    subObTableOperationResult.setExecuteHost(subObTable.getIp());
                    subObTableOperationResult.setExecutePort(subObTable.getPort());
                    results[subOperationWithIndexList.get(i).getLeft()] = subObTableOperationResult;
                }
            }
        }
        String endpoint = subObTable.getIp() + ":" + subObTable.getPort();
        MonitorUtil.info(subRequest, subObTable.getDatabase(), tableName,
            "BATCH-partitionExecute-", endpoint, subOperations, partId,
            subObTableOperationResults.size(), endExecute - startExecute,
            obTableClient.getslowQueryMonitorThreshold());//记录执行监控信息
    }

    /*
     * Execute internal.
     */
    public ObTableBatchOperationResult executeInternal() throws Exception {
        //表名长度不为空
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException("table name is null");
        }
        //开始时间
        long start = System.currentTimeMillis();
        //获取所有待执行的操作列表
        List<ObTableOperation> operations = batchOperation.getTableOperations();
        //创建结果列表数组
        ObTableOperationResult[] obTableOperationResults = null;
        if (returnOneResult) {
            obTableOperationResults = new ObTableOperationResult[1];
        } else {
            obTableOperationResults = new ObTableOperationResult[operations.size()];
        }
        //解析出分区 键是分区标识符，值是一个包含分区参数和操作列表的对
        Map<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> partitions = partitionPrepare();
        //
        long getTableTime = System.currentTimeMillis();
        //上下文
        final Map<Object, Object> context = ThreadLocalMap.getContextMap();

        if (executorService != null && !executorService.isShutdown() && partitions.size() > 1) {
            final ConcurrentTaskExecutor executor = new ConcurrentTaskExecutor(executorService,
                partitions.size());
            //并发执行
            for (final Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> entry : partitions
                .entrySet()) {
                ObTableOperationResult[] finalObTableOperationResults = obTableOperationResults;
                executor.execute(new ConcurrentTask() {
                    /*
                     * Do task.
                     */
                    @Override
                    public void doTask() {
                        try {
                            ThreadLocalMap.transmitContextMap(context);
                            partitionExecute(finalObTableOperationResults, entry);
                        } catch (Exception e) {
                            logger.error(LCD.convert("01-00026"), e);
                            executor.collectExceptions(e);
                        } finally {
                            ThreadLocalMap.reset();
                        }
                    }
                });
            }
            //批量等待时间 （每1ms检查一次）
            long estimate = obTableClient.getRuntimeBatchMaxWait() * 1000L * 1000L;
            try {
                while (estimate > 0) {
                    long nanos = System.nanoTime();
                    try {
                        executor.waitComplete(1, TimeUnit.MILLISECONDS);
                    } catch (InterruptedException e) {
                        throw new ObTableUnexpectedException(
                            "Batch Concurrent Execute interrupted", e);
                    }

                    if (executor.getThrowableList().size() > 0) {
                        throw new ObTableUnexpectedException("Batch Concurrent Execute Error",
                            executor.getThrowableList().get(0));
                    }

                    if (executor.isComplete()) {
                        break;
                    }

                    estimate = estimate - (System.nanoTime() - nanos);
                }
            } finally {
                executor.stop();
            }

            if (executor.getThrowableList().size() > 0) {
                throw new ObTableUnexpectedException("Batch Concurrent Execute Error", executor
                    .getThrowableList().get(0));
            }

            if (!executor.isComplete()) {
                throw new ObTableUnexpectedException("Batch Concurrent Execute Error ["
                                                     + obTableClient.getRpcExecuteTimeout()
                                                     + "]/ms");
            }

        } else {
            for (final Map.Entry<Long, ObPair<ObTableParam, List<ObPair<Integer, ObTableOperation>>>> entry : partitions
                .entrySet()) {
                //执行操作
                partitionExecute(obTableOperationResults, entry);
            }
        }
        //执行结果
        ObTableBatchOperationResult batchOperationResult = new ObTableBatchOperationResult();
        for (ObTableOperationResult obTableOperationResult : obTableOperationResults) {
            batchOperationResult.addResult(obTableOperationResult);
        }
        //记录执行时间
        MonitorUtil.info(batchOperationResult, obTableClient.getDatabase(), tableName, "BATCH", "",
            obTableOperationResults.length, getTableTime - start, System.currentTimeMillis()
                                                                  - getTableTime,
            obTableClient.getslowQueryMonitorThreshold());

        return batchOperationResult;
    }

    /*
     * clear batch operations1
     */
    public void clear() {
        batchOperation = new ObTableBatchOperation();
    }

    /*
     * Set executor service.
     */
    public void setExecutorService(ExecutorService executorService) {
        this.executorService = executorService;
    }

    public boolean isReturningAffectedEntity() {
        return returningAffectedEntity;
    }

    public void setReturningAffectedEntity(boolean returningAffectedEntity) {
        this.returningAffectedEntity = returningAffectedEntity;
    }
}
