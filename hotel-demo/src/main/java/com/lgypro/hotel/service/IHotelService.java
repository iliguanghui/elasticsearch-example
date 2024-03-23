package com.lgypro.hotel.service;

import com.baomidou.mybatisplus.extension.service.IService;
import com.lgypro.hotel.pojo.Hotel;
import com.lgypro.hotel.pojo.PageResult;
import com.lgypro.hotel.pojo.RequestParams;

import java.util.List;
import java.util.Map;

public interface IHotelService extends IService<Hotel> {

    PageResult search(RequestParams params);

    Map<String, List<String>> getFilters(RequestParams params);

    List<String> getSuggestions(String key);

    void upsertById(Long id);

    void deleteById(Long id);
}
