package com.dygstudio.myblog.service.controller;

import com.dygstudio.myblog.service.entity.User;
import com.dygstudio.myblog.service.service.UserService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * 〈功能概述〉
 *
 * @className: UserController
 * @package: com.dygstudio.myblog.service.controller
 * @author: diyaguang
 * @date: 2020/9/4 5:11 下午
 */
@RestController
@RequestMapping("/api/user")
public class UserController {

    @Resource
    UserService userService;

    @ResponseBody
    @RequestMapping(value = "/list")
    public List<User> getUserList(){
        return userService.list();
    }
}
