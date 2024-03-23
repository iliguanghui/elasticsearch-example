package com.lgypro.hotel.pojo;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class HotelSyncMessage {

    public enum OpType {
        UPSERT,
        DELETE
    }

    OpType type;
    Long id;
}
