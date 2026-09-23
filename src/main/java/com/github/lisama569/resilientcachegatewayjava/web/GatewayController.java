package com.github.lisama569.resilientcachegatewayjava.web;

import com.github.lisama569.resilientcachegatewayjava.model.TargetResult;
import com.github.lisama569.resilientcachegatewayjava.service.CacheGatewayService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 数据读取入口。
 */
@RestController
@RequestMapping("/api")
public class GatewayController {

    private final CacheGatewayService service;

    public GatewayController(CacheGatewayService service) {
        this.service = service;
    }

    @GetMapping("/targets/{target}")
    public TargetResult getTarget(@PathVariable String target) {
        return service.fetch(target);
    }
}
