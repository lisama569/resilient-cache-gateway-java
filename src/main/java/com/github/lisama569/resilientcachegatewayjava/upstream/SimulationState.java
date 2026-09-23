package com.github.lisama569.resilientcachegatewayjava.upstream;

import java.util.Map;
import java.util.Set;

/**
 * 模拟上游的可变运行态：每个目标的延迟、故障标记、慢调用标记与调用计数。
 * 支持运行期通过 admin 接口调整，以便本地复现各种故障场景。
 */
public interface SimulationState {
    long delayMillis(String target);
    boolean isFailing(String target);
    boolean isSlow(String target);

    long callCount(String target);
    long totalCallCount();

    void setDelay(String target, long delayMillis);
    void setFailing(String target, boolean failing);
    void setSlow(String target, boolean slow);
    void reset();

    /** 快照，用于 admin 查询与测试断言。 */
    Snapshot snapshot();

    record Snapshot(Map<String, Long> delays, Set<String> failingKeys,
                    Set<String> slowKeys, Map<String, Long> callCounts,
                    long totalCalls) {
    }
}
