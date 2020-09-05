package com.dygstudio.myblog.service.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.dygstudio.myblog.service.entity.Dictionary;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public interface DictionaryService extends IService<Dictionary> {
    List<Dictionary> getAllDictionary();
    List<Dictionary> getByTopDictionary();
    Dictionary getDictionaryById(String id);
    Dictionary getDictionaryByValue(String value);
    List<Dictionary> getDictionaryByPId(String pid);
}
