package com.lgypro.hotel.pojo;

import lombok.Data;

import java.util.List;

@Data
public class PageResult {
    private Long total;
    private List<Hotel> hotels;

    public PageResult(Long total, List<Hotel> hotels) {
        this.total = total;
        this.hotels = hotels;
    }
}
