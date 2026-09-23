package com.github.lisama569.resilientcachegatewayjava.core;

/**
 * 时间源抽象，便于测试中精确控制过期与熔断时间。
 */
public interface TimeSource {
    long nowMillis();
    long nowNanos();

    static TimeSource system() {
        return new TimeSource() {
            @Override public long nowMillis() { return System.currentTimeMillis(); }
            @Override public long nowNanos() { return System.nanoTime(); }
        };
    }
}
