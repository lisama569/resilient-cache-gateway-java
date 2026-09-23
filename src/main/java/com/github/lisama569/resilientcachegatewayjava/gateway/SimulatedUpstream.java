package com.github.lisama569.resilientcachegatewayjava.gateway;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 本地可配置延迟/故障的模拟上游，代替真实外部依赖。
 * 延迟与故障开关通过 {@link GatewayProperties} 配置，运行期可通过控制器调整。
 */
@Component
public class SimulatedUpstream {

    private static final Logger log = LoggerFactory.getLogger(SimulatedUpstream.class);

    private final GatewayProperties properties;
    private final AtomicLong callSequence = new AtomicLong();

    public SimulatedUpstream(GatewayProperties properties) {
        this.properties = properties;
    }

    /**
     * 模拟一次上游读取：按配置睡眠固定延迟，故障开关打开时抛出异常。
     *
     * @param key 目标 key
     * @return 带调用序号的数据，便于测试判断"只回源一次"
     */
    public String fetch(String key) {
        long seq = callSequence.incrementAndGet();
        long latency = properties.getSimulatedLatencyMillis();
        log.info("[upstream] fetch key={} seq={} latencyMs={} failure={}",
                key, seq, latency, properties.isSimulatedFailure());
        if (latency > 0) {
            try {
                Thread.sleep(latency);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new UpstreamException("interrupted while fetching key=" + key, e);
            }
        }
        if (properties.isSimulatedFailure()) {
            throw new UpstreamException("simulated upstream failure for key=" + key);
        }
        return "value-of-" + key + "#" + seq;
    }

    /** 上游调用总次数（用于测试断言回源次数）。 */
    public long getCallCount() {
        return callSequence.get();
    }

    /** 上游异常类型。 */
    public static class UpstreamException extends RuntimeException {
        public UpstreamException(String message) {
            super(message);
        }

        public UpstreamException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
