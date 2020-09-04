package com.dygstudio.myblog.service.common;

import java.util.UUID;

/**
 * 〈功能概述〉
 *
 * @className: CommonUtils
 * @package: com.dygstudio.myblog.service.common
 * @author: diyaguang
 * @date: 2020/9/4 3:32 下午
 */
public class CommonUtils {
    public static String GenerateId(){
        return UUID.randomUUID().toString().replaceAll("-","");
    }

}
