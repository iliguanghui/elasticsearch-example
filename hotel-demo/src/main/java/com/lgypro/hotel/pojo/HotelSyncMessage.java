package com.lgypro.hotel.pojo;

import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class HotelSyncMessage {

    public enum OpType {
        UPSERT,
        DELETE
    }

    OpType type;
    Long id;
}
