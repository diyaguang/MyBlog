package com.dygstudio.myblog.service.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * 〈功能概述〉
 *
 * @className: CommonConfig
 * @package: com.dygstudio.myblog.service.config
 * @author: diyaguang
 * @date: 2020/9/4 3:57 下午
 */
@Configuration
@ComponentScan(value = "com.dygstudio.myblog.service.entity")
public class CommonConfig {
}
