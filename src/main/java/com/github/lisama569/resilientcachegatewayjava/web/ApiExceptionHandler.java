package com.github.lisama569.resilientcachegatewayjava.web;

import com.github.lisama569.resilientcachegatewayjava.exception.RequestRejectedException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把显式拒绝翻译成确定的 HTTP 语义，响应体始终带 status/reason/message，
 * 调用方既可按 HTTP 状态码也可按 reason 字段区分判定，绝不静默丢失。
 *
 * 每热点过载 / 全局过载 / 排队超时 -> 429 TOO_MANY_REQUESTS；
 * 熔断打开 / 上游超时 / 上游失败且无兜底 -> 503 SERVICE_UNAVAILABLE。
 */
@RestControllerAdvice
public class ApiExceptionHandler {

    @ExceptionHandler(RequestRejectedException.class)
    public ResponseEntity<Map<String, Object>> handleRejected(RequestRejectedException ex) {
        HttpStatus http = switch (ex.reason()) {
            case PER_KEY_OVERLOADED, GLOBAL_OVERLOADED, QUEUE_TIMEOUT -> HttpStatus.TOO_MANY_REQUESTS;
            case CIRCUIT_OPEN, UPSTREAM_TIMEOUT, UPSTREAM_FAILURE -> HttpStatus.SERVICE_UNAVAILABLE;
        };
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("target", ex.target());
        body.put("status", "REJECTED");
        body.put("reason", ex.reason().name());
        body.put("message", ex.getMessage());
        return ResponseEntity.status(http).body(body);
    }
}
