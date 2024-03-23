package com.lgypro.hotel.processor;

import com.lgypro.hotel.pojo.HotelSyncMessage;
import com.lgypro.hotel.pojo.HotelSyncMessage.OpType;
import com.lgypro.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HotelSyncMessageProcessor {

    @Autowired
    private IHotelService hotelService;

    public void process(HotelSyncMessage message) {
        if (message.getType() == OpType.UPSERT) {
            hotelService.upsertById(message.getId());
        }
        if (message.getType() == OpType.DELETE) {
            hotelService.deleteById(message.getId());
        }
    }
}
