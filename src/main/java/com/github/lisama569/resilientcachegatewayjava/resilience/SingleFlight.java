package com.github.lisama569.resilientcachegatewayjava.resilience;

import com.github.lisama569.resilientcachegatewayjava.core.TimeSource;
import com.github.lisama569.resilientcachegatewayjava.exception.RequestRejectedException;
import com.github.lisama569.resilientcachegatewayjava.model.RejectReason;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

/**
 * 单飞（请求合并）+ 每 key 并发舱壁。
 *
 * 不变量：
 *  1. 同一 key 同一时刻只有一个实际加载者（leader），其余请求（waiter）
 *     共享同一个 CompletableFuture，上游只被打一次；
 *  2. 参与者（含 leader）达到 perKeyMaxConcurrent 后，新请求立即以
 *     PER_KEY_OVERLOADED 被拒绝，拒绝规则对所有请求一致；
 *  3. waiter 等待超过 acquireTimeoutMillis 以 QUEUE_TIMEOUT 拒绝，
 *     不会无限堆积；leader 完成后立即放行下一轮新请求。
 */
public class SingleFlight<V> {

    private final ConcurrentHashMap<String, Flight<V>> flights = new ConcurrentHashMap<>();
    private final int maxConcurrent;
    private final long acquireTimeoutMillis;
    private final TimeSource timeSource;

    public SingleFlight(int maxConcurrent, long acquireTimeoutMillis, TimeSource timeSource) {
        this.maxConcurrent = Math.max(1, maxConcurrent);
        this.acquireTimeoutMillis = acquireTimeoutMillis;
        this.timeSource = timeSource;
    }

    /**
     * 以合并方式执行 loader。
     *
     * @param key              合并键
     * @param loader           仅由 leader 执行一次
     * @param rejectedObserver 拒绝原因回调（打点用）
     */
    public V call(String key, Callable<V> loader,
                  Consumer<RejectReason> rejectedObserver) {
        Flight<V> candidate = new Flight<>(timeSource.nowMillis());
        boolean[] overloaded = {false};

        // compute 内对单个 bin 加锁，"检查旧组/建组/入组/计数"原子完成。
        // 注意：是否为 leader 取决于"本线程的 candidate 是否成为组对象"（身份比较），
        // 绝不能把 leader 标记放在共享组对象上，否则等待者也会误判为 leader。
        Flight<V> flight = flights.compute(key, (k, existing) -> {
            if (existing == null) {
                return candidate;
            }
            synchronized (existing) {
                if (existing.future.isDone()) {
                    // 旧组刚好在收尾：本轮整体替换为新组，当前线程成为 leader
                    return candidate;
                }
                if (existing.participants >= maxConcurrent) {
                    overloaded[0] = true;
                    return existing;
                }
                existing.participants++;
                return existing;
            }
        });

        if (overloaded[0]) {
            rejectedObserver.accept(RejectReason.PER_KEY_OVERLOADED);
            throw new RequestRejectedException(key, RejectReason.PER_KEY_OVERLOADED,
                    "hot key overloaded: participants limit " + maxConcurrent + " reached for " + key);
        }

        boolean leader = (flight == candidate);
        if (leader) {
            return runAsLeader(key, flight, loader);
        }
        return awaitResult(key, flight, rejectedObserver);
    }

    private V runAsLeader(String key, Flight<V> flight, Callable<V> loader) {
        try {
            V result = loader.call();
            flight.future.complete(result);
            return result;
        } catch (RuntimeException | Error ex) {
            flight.future.completeExceptionally(ex);
            throw ex;
        } catch (Exception ex) {
            flight.future.completeExceptionally(ex);
            throw new RuntimeException(ex);
        } finally {
            flights.remove(key, flight);
            synchronized (flight) {
                flight.notifyAll();
            }
        }
    }

    private V awaitResult(String key, Flight<V> flight, Consumer<RejectReason> rejectedObserver) {
        // 排队超时是真实墙钟时间（Object.wait 按系统时钟调度），不能用可被测试冻结的时钟
        long deadline = System.currentTimeMillis() + acquireTimeoutMillis;
        synchronized (flight) {
            try {
                while (!flight.future.isDone()) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        rejectedObserver.accept(RejectReason.QUEUE_TIMEOUT);
                        throw new RequestRejectedException(key, RejectReason.QUEUE_TIMEOUT,
                                "queue wait timeout after " + acquireTimeoutMillis + "ms for " + key);
                    }
                    flight.wait(remaining);
                }
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RequestRejectedException(key, RejectReason.QUEUE_TIMEOUT,
                        "interrupted while waiting for in-flight load of " + key);
            }
        }
        try {
            return flight.future.get();
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RequestRejectedException(key, RejectReason.QUEUE_TIMEOUT,
                    "interrupted while resolving in-flight load of " + key);
        } catch (ExecutionException ex) {
            Throwable cause = ex.getCause();
            if (cause instanceof RuntimeException re) {
                throw re;
            }
            throw new RuntimeException(cause);
        }
    }

    /** 当前存在合并组的 key 数（监控用）。 */
    public int activeKeys() {
        return flights.size();
    }

    /**
     * 一个合并组。leader 创建时隐式占 1 个参与者名额。
     */
    private static final class Flight<V> {
        final CompletableFuture<V> future = new CompletableFuture<>();
        final long startedAt;
        int participants = 1;

        Flight(long startedAt) {
            this.startedAt = startedAt;
        }
    }
}
