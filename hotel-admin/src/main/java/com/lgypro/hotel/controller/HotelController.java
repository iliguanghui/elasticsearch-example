package com.lgypro.hotel.controller;

import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.lgypro.hotel.pojo.Hotel;
import com.lgypro.hotel.pojo.HotelSyncMessage;
import com.lgypro.hotel.pojo.HotelSyncMessage.OpType;
import com.lgypro.hotel.pojo.PageResult;
import com.lgypro.hotel.service.HotelSyncMessageSender;
import com.lgypro.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;

import java.security.InvalidParameterException;

@RestController
@RequestMapping("/hotel")
public class HotelController {

    @Autowired
    private IHotelService hotelService;

    @Autowired
    private HotelSyncMessageSender messageSender;


    @Value("${sync.enable-produce-hotel-sync-messages}")
    private boolean enableProduceHotelSyncMessages;

    @GetMapping("/{id}")
    public Hotel queryById(@PathVariable("id") Long id) {
        return hotelService.getById(id);
    }

    @GetMapping("/list")
    public PageResult hotelList(
        @RequestParam(value = "page", defaultValue = "1") Integer page,
        @RequestParam(value = "size", defaultValue = "1") Integer size
    ) {
        Page<Hotel> result = hotelService.page(new Page<>(page, size));

        return new PageResult(result.getTotal(), result.getRecords());
    }

    @PostMapping
    public void saveHotel(@RequestBody Hotel hotel) {
        hotelService.save(hotel);
        if (enableProduceHotelSyncMessages) {
            messageSender.sendMessage(new HotelSyncMessage(OpType.UPSERT, hotel.getId()));
        }
    }

    @PutMapping()
    public void updateById(@RequestBody Hotel hotel) {
        if (hotel.getId() == null) {
            throw new InvalidParameterException("id不能为空");
        }
        hotelService.updateById(hotel);
        if (enableProduceHotelSyncMessages) {
            messageSender.sendMessage(new HotelSyncMessage(OpType.UPSERT, hotel.getId()));
        }
    }

    @DeleteMapping("/{id}")
    public void deleteById(@PathVariable("id") Long id) {
        hotelService.removeById(id);
        if (enableProduceHotelSyncMessages) {
            messageSender.sendMessage(new HotelSyncMessage(OpType.DELETE, id));
        }
    }
}
