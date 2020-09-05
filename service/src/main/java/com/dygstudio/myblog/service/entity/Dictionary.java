package com.dygstudio.myblog.service.entity;

import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;

import java.io.Serializable;
import java.util.List;

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
    private String pid;
    private Integer Status;
    private Integer IsDel;

    @TableField(exist = false)
    private List<Dictionary> childs;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
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

    public List<Dictionary> getChilds() {
        return childs;
    }

    public void setChilds(List<Dictionary> childs) {
        this.childs = childs;
    }

    public Integer getStatus() {
        return Status;
    }

    public void setStatus(Integer status) {
        Status = status;
    }

    public Integer getIsDel() {
        return IsDel;
    }

    public void setIsDel(Integer isDel) {
        IsDel = isDel;
    }
}
