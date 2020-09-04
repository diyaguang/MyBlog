package com.dygstudio.myblog.service.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dygstudio.myblog.service.entity.User;
import com.dygstudio.myblog.service.mapper.UserMapper;
import com.dygstudio.myblog.service.service.UserService;
import org.springframework.stereotype.Service;

/**
 * 〈功能概述〉
 *
 * @className: UserServiceImpl
 * @package: com.dygstudio.myblog.service.service.impl
 * @author: diyaguang
 * @date: 2020/9/4 4:48 下午
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements UserService {
}
