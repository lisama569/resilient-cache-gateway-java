package com.github.lisama569.resilientcachegatewayjava.upstream;

import com.github.lisama569.resilientcachegatewayjava.core.TimeSource;
import com.github.lisama569.resilientcachegatewayjava.exception.UpstreamException;
import com.github.lisama569.resilientcachegatewayjava.model.UpstreamResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 模拟上游：
 *  - failing 标记：睡眠延迟后抛 {@link UpstreamException}（代表 5xx 业务失败）；
 *  - 否则睡眠 delayMillis 后返回确定性的 value（便于断言）；
 *  - 超时不在这里抛出：延迟是否超过 upstreamTimeoutMillis 由服务层的
 *    Future + timeout 统一裁决，保证"超时"与"普通失败"可区分。
 */
public class SimulatedUpstreamClient implements UpstreamClient {

    private static final Logger log = LoggerFactory.getLogger(SimulatedUpstreamClient.class);

    private final SimulationState state;
    private final TimeSource timeSource;

    public SimulatedUpstreamClient(SimulationState state, TimeSource timeSource) {
        this.state = state;
        this.timeSource = timeSource;
    }

    @Override
    public UpstreamResponse fetch(String target) throws Exception {
        long delay = state.delayMillis(target);
        boolean failing = state.isFailing(target);
        if (state instanceof DefaultSimulationState d) {
            d.recordCall(target);
        }
        log.info("[upstream] fetch start target={}, delayMillis={}, failing={}", target, delay, failing);
        if (delay > 0) {
            Thread.sleep(delay);
        }
        if (failing) {
            log.info("[upstream] fetch FAIL target={} (simulated upstream error)", target);
            throw new UpstreamException("simulated upstream failure for target=" + target);
        }
        String value = "value-of-" + target + "-at-" + timeSource.nowMillis();
        log.info("[upstream] fetch OK target={}", target);
        return new UpstreamResponse(target, value);
    }
}
