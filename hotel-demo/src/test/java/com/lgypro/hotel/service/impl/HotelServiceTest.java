package com.lgypro.hotel.service.impl;

import com.lgypro.hotel.pojo.RequestParams;
import com.lgypro.hotel.service.IHotelService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.Map;

@SpringBootTest
class HotelServiceTest {
    @Autowired
    private IHotelService hotelService;

    @Test
    void testFilters() {
        Map<String, List<String>> result = hotelService.getFilters(new RequestParams());
        result.forEach((key, value) -> {
            System.out.printf("%s: %s%n", key, value);
        });
    }
}