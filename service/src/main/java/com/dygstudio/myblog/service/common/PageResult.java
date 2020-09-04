package com.dygstudio.myblog.service.common;

import java.util.List;

/**
 * 〈功能概述〉
 *
 * @className: PageResult
 * @package: com.dygstudio.myblog.service.common
 * @author: diyaguang
 * @date: 2020/9/4 3:55 下午
 */
public class PageResult<T> {
    private String code; //状态码, 0表示成功
    private String msg;  //提示信息
    private long count; // 总数量, bootstrapTable是total
    private List<T> data; // 当前数据, bootstrapTable是rows

    public PageResult() {
    }
    public PageResult(List<T> rows) {
        this.data = rows;
        this.count = rows.size();
        this.code = SysConstant.RESULT_CODE_SUCCESSFUL;
        this.msg = "";
    }

    public PageResult(long total, List<T> rows) {
        this.count = total;
        this.data = rows;
        this.code = SysConstant.RESULT_CODE_SUCCESSFUL;
        this.msg = "";
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public List<T> getData() {
        return data;
    }

    public void setData(List<T> data) {
        this.data = data;
        //this.count = data.size();
    }
}
