package com.dygstudio.myblog.service.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;

/**
 * 〈功能概述〉
 *
 * @className: Dictionary
 * @package: com.dygstudio.myblog.service.entity
 * @author: diyaguang
 * @date: 2020/9/4 4:17 下午
 */
@TableName("Dictionary")
public class Dictionary implements Serializable {
    @TableId
    private String id;
    private String name;
    private String code;
    private Integer sort;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public Integer getSort() {
        return sort;
    }

    public void setSort(Integer sort) {
        this.sort = sort;
    }
}
