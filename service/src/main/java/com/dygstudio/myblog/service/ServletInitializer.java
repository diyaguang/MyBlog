package com.dygstudio.myblog.service;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

/**
 * 〈功能概述〉
 *
 * @className: ServletInitializer
 * @package: com.dygstudio.myblog.service
 * @author: diyaguang
 * @date: 2020/9/4 4:21 下午
 */
public class ServletInitializer extends SpringBootServletInitializer {
    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(ServiceApplication.class);
    }
}
