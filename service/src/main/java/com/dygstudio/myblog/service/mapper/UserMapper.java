package com.dygstudio.myblog.service.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dygstudio.myblog.service.entity.User;
import org.apache.ibatis.annotations.Mapper;

/**
 * 〈功能概述〉
 *
 * @className: UserMapper
 * @package: com.dygstudio.myblog.service.mapper
 * @author: diyaguang
 * @date: 2020/9/4 4:25 下午
 */
@Mapper
public interface UserMapper extends BaseMapper<User> {
}
