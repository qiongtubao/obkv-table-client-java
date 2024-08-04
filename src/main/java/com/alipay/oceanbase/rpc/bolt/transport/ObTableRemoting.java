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

package com.alipay.oceanbase.rpc.bolt.transport;

import com.alipay.oceanbase.rpc.bolt.protocol.ObTablePacket;
import com.alipay.oceanbase.rpc.bolt.protocol.ObTablePacketCode;
import com.alipay.oceanbase.rpc.exception.*;
import com.alipay.oceanbase.rpc.protocol.packet.ObCompressType;
import com.alipay.oceanbase.rpc.protocol.payload.AbstractPayload;
import com.alipay.oceanbase.rpc.protocol.payload.Credentialable;
import com.alipay.oceanbase.rpc.protocol.payload.ObPayload;
import com.alipay.oceanbase.rpc.protocol.payload.ObRpcResultCode;
import com.alipay.oceanbase.rpc.protocol.payload.impl.execute.ObTableOperationRequest;
import com.alipay.oceanbase.rpc.protocol.payload.impl.login.ObTableLoginRequest;
import com.alipay.oceanbase.rpc.util.ObPureCrc32C;
import com.alipay.oceanbase.rpc.util.TableClientLoggerFactory;
import com.alipay.oceanbase.rpc.util.TraceUtil;
import com.alipay.remoting.*;
import com.alipay.remoting.exception.RemotingException;
import io.netty.buffer.ByteBuf;
import latte.lib.api.monitor.Transaction;
import latte.log.LatteMonitor;
import org.slf4j.Logger;

import static com.alipay.oceanbase.rpc.protocol.packet.ObCompressType.INVALID_COMPRESSOR;
import static com.alipay.oceanbase.rpc.protocol.packet.ObCompressType.NONE_COMPRESSOR;

public class ObTableRemoting extends BaseRemoting {

    private static final Logger logger = TableClientLoggerFactory.getLogger(ObTableRemoting.class);

    /*
     * Ob table remoting.
     */
    public ObTableRemoting(CommandFactory commandFactory) {
        super(commandFactory);
    }

    /*
     * Invoke sync.
     */
    public ObPayload invokeSync(final ObTableConnection conn, final ObPayload request,
                                final int timeoutMillis) throws RemotingException,
                                                        InterruptedException {

        request.setSequence(conn.getNextSequence());//设置序列号和唯一标识
        request.setUniqueId(conn.getUniqueId());

        if (request instanceof Credentialable) {
            if (conn.getCredential() == null) {//如果原来没有凭证报错
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "credential is null");
                logger.warn(errMessage);
                throw new ObTableUnexpectedException(errMessage);
            }
            ((Credentialable) request).setCredential(conn.getCredential());//设置凭据
        }
        if (request instanceof ObTableLoginRequest) {//如果是登陆请求 则租户id为1 这通常表示系统租户。
            // setting sys tenant in rpc header when login
            ((ObTableLoginRequest) request).setTenantId(1);
        } else if (request instanceof AbstractPayload) {//设置请求的租户 ID 为连接的租户 ID。
            ((AbstractPayload) request).setTenantId(conn.getTenantId());
        }

        ObTablePacket obRequest = this.getCommandFactory().createRequestCommand(request);//创建请求
        Transaction transaction = null;
        try {
            ObTableOperationRequest request1 = ((ObTableOperationRequest) request);
            transaction = LatteMonitor.getTransaction("obtable.remoting.rpc."
                                                      + request1.getTableOperation()
                                                          .getOperationType().name() + "."
                                                      + conn.getConnection().getRemoteIP() + ":"
                                                      + conn.getConnection().getRemotePort());
        } catch (Exception e) {

        }
        ObTablePacket response;
        try {
            response = (ObTablePacket) super.invokeSync(conn.getConnection(), obRequest,
                timeoutMillis); //使用 super.invokeSync 方法发送请求，并等待响应。超时时间为 timeoutMillis。
            if (response == null) {//响应为空 则记录警告日志并抛出ObTableTransportException
                String errMessage = TraceUtil
                    .formatTraceMessage(conn, request, "get null response");
                logger.warn(errMessage);
                ExceptionUtil.throwObTableTransportException(errMessage,
                    TransportCodes.BOLT_RESPONSE_NULL);
                if (transaction != null)
                    transaction.setFail(new Exception(errMessage));
                return null;
            } else if (!response.isSuccess()) {//响应是否成功。如果不成功，则记录警告日志并抛出异常。
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "get an error response: " + response.getMessage());
                logger.warn(errMessage);
                response.releaseByteBuf();
                ExceptionUtil.throwObTableTransportException(errMessage,
                    response.getTransportCode());
                if (transaction != null)
                    transaction.setFail(new Exception(errMessage));
                return null;
            }
            if (transaction != null)
                transaction.setSuccess();
        } finally {
            if (transaction != null)
                transaction.complete();
        }

        try {
            // decode packet header first
            response.decodePacketHeader();//解码包头，获取压缩类型等信息。
            ObCompressType compressType = response.getHeader().getObCompressType();
            if (compressType != INVALID_COMPRESSOR && compressType != NONE_COMPRESSOR) { //如果压缩类型不是 INVALID_COMPRESSOR 也不是 NONE_COMPRESSOR 则抛出 FeatureNotSupportedException
                String errMessage = TraceUtil.formatTraceMessage(
                    conn,
                    request,
                    "Rpc Result is compressed. Java Client is not supported. msg:"
                            + response.getMessage());
                logger.warn(errMessage);
                throw new FeatureNotSupportedException(errMessage);
            }
            ByteBuf buf = response.getPacketContentBuf();
            // If response indicates the request is routed to wrong server, we should refresh the routing meta.
            if (response.getHeader().isRoutingWrong()) { //处理路由错误
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "routed to the wrong server: " + response.getMessage());
                logger.warn(errMessage);
                throw new ObTableRoutingWrongException(errMessage);
            }

            // verify checksum
            long expected_checksum = response.getHeader().getChecksum(); //验证校验和
            byte[] content = new byte[buf.readableBytes()];
            buf.getBytes(buf.readerIndex(), content);
            if (ObPureCrc32C.calculate(content) != expected_checksum) {
                String errMessage = TraceUtil.formatTraceMessage(conn, request,
                    "get response with checksum error: " + response.getMessage());
                logger.warn(errMessage);
                ExceptionUtil.throwObTableTransportException(errMessage,
                    TransportCodes.BOLT_CHECKSUM_ERR);
                return null;
            }

            // decode ResultCode for response packet
            ObRpcResultCode resultCode = new ObRpcResultCode();
            resultCode.decode(buf);

            if (resultCode.getRcode() != 0) {
                ExceptionUtil.throwObTableException(conn.getObTable().getIp(), conn.getObTable()
                    .getPort(), response.getHeader().getTraceId1(), response.getHeader()
                    .getTraceId0(), resultCode.getRcode(), resultCode.getErrMsg());
                return null;
            }

            // decode payload itself
            ObPayload payload;
            if (response.getCmdCode() instanceof ObTablePacketCode) {//创造解析器
                payload = ((ObTablePacketCode) response.getCmdCode()).newPayload(response
                    .getHeader());
                payload.setSequence(response.getHeader().getTraceId1());
                payload.setUniqueId(response.getHeader().getTraceId0());
            } else {
                String errMessage = TraceUtil.formatTraceMessage(conn, response,
                    "receive unexpected command code: " + response.getCmdCode().value());
                throw new ObTableUnexpectedException(errMessage);
            }

            payload.decode(buf);//解析数据
            return payload;
        } finally {
            // Very important to release ByteBuf memory
            response.releaseByteBuf();
        }
    }

    @Override
    protected InvokeFuture createInvokeFuture(RemotingCommand request, InvokeContext invokeContext) {
        return new ObClientFuture(request.getId());
    }

    @Override
    protected InvokeFuture createInvokeFuture(Connection conn, RemotingCommand request,
                                              InvokeContext invokeContext,
                                              InvokeCallback invokeCallback) {
        return new ObClientFuture(request.getId());
    }

}
