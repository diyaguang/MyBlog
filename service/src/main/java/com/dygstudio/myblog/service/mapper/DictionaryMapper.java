package com.dygstudio.myblog.service.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.dygstudio.myblog.service.entity.Dictionary;
import org.apache.ibatis.annotations.Mapper;

import java.util.List;

/**
 * 〈功能概述〉
 *
 * @className: DictionaryMapper
 * @package: com.dygstudio.myblog.service.mapper
 * @author: diyaguang
 * @date: 2020/9/4 4:40 下午
 */
@Mapper
public interface DictionaryMapper extends BaseMapper<Dictionary> {
    List<Dictionary> getAllDictionary();
    List<Dictionary> getByTopDictionary();
    Dictionary getDictionaryById(String id);
    Dictionary getDictionaryByValue(String value);
    List<Dictionary> getDictionaryByPId(String pid);
}
