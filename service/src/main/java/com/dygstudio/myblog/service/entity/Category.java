package com.dygstudio.myblog.service.entity;

import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;

/**
 * 〈功能概述〉
 *
 * @className: Category
 * @package: com.dygstudio.myblog.service.entity
 * @author: diyaguang
 * @date: 2020/9/4 4:14 下午
 */
@TableName("Category")
public class Category implements Serializable {
    @TableId
    private String id;
    private String name;
    private String pid;
    private Integer status;
    private Integer isdel;

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

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Integer getIsdel() {
        return isdel;
    }

    public void setIsdel(Integer isdel) {
        this.isdel = isdel;
    }
}
