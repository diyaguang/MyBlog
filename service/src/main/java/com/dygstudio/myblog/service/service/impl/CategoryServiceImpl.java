package com.dygstudio.myblog.service.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dygstudio.myblog.service.entity.Category;
import com.dygstudio.myblog.service.mapper.CategoryMapper;
import com.dygstudio.myblog.service.service.CategoryService;
import org.springframework.stereotype.Service;

/**
 * 〈功能概述〉
 *
 * @className: CategoryServiceImpl
 * @package: com.dygstudio.myblog.service.service.impl
 * @author: diyaguang
 * @date: 2020/9/4 4:50 下午
 */
@Service
public class CategoryServiceImpl extends ServiceImpl<CategoryMapper, Category> implements CategoryService {
}
