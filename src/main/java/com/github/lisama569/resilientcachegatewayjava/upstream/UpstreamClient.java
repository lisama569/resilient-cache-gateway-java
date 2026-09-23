package com.github.lisama569.resilientcachegatewayjava.upstream;

import com.github.lisama569.resilientcachegatewayjava.model.UpstreamResponse;

/**
 * 上游数据读取抽象。生产环境可替换为真实 HTTP/RPC 客户端，
 * 本地验证使用可配置延迟与故障的模拟实现。
 */
public interface UpstreamClient {
    UpstreamResponse fetch(String target) throws Exception;
}
