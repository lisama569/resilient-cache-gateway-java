package com.github.lisama569.resilientcachegatewayjava.tests;

import com.github.lisama569.resilientcachegatewayjava.core.TimeSource;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 测试用可控时间源：毫秒时钟可手动推进，纳秒时钟真实流逝（用于耗时测量）。
 */
public class MutableTimeSource implements TimeSource {

    private final AtomicLong millis = new AtomicLong(System.currentTimeMillis());

    public void advanceMillis(long delta) {
        millis.addAndGet(delta);
    }

    public void setMillis(long value) {
        millis.set(value);
    }

    @Override
    public long nowMillis() {
        return millis.get();
    }

    @Override
    public long nowNanos() {
        return System.nanoTime();
    }
}
