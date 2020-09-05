package com.dygstudio.myblog.service.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.dygstudio.myblog.service.entity.Dictionary;
import com.dygstudio.myblog.service.mapper.DictionaryMapper;
import com.dygstudio.myblog.service.service.DictionaryService;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * 〈功能概述〉
 *
 * @className: DictionaryServiceImpl
 * @package: com.dygstudio.myblog.service.service.impl
 * @author: diyaguang
 * @date: 2020/9/4 4:51 下午
 */
@Service
public class DictionaryServiceImpl extends ServiceImpl<DictionaryMapper, Dictionary> implements DictionaryService {
    public  List<Dictionary> getAllDictionary(){
        return null;
    }
    public List<Dictionary> getByTopDictionary(){
        return null;
    }
    public Dictionary getDictionaryById(String id){
        return null;
    }
    public Dictionary getDictionaryByValue(String value){
        return null;
    }
    public List<Dictionary> getDictionaryByPId(String pid){
        return null;
    }
}
