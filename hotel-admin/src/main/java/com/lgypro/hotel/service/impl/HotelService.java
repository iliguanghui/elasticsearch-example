package com.lgypro.hotel.service.impl;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lgypro.hotel.mapper.HotelMapper;
import com.lgypro.hotel.pojo.Hotel;
import com.lgypro.hotel.service.IHotelService;
import org.springframework.stereotype.Service;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {
}
