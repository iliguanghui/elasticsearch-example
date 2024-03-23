package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import com.lgypro.hotel.pojo.Hotel;
import com.lgypro.hotel.pojo.HotelDoc;
import com.lgypro.hotel.service.IHotelService;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
@Disabled
public class HotelDocumentTest {

    @Autowired
    private IHotelService hotelService;

    @Autowired
    private ElasticsearchClient client;

    @Test
    void testAddDocuments() {
        List<Hotel> hotelList = hotelService.list();
        long startTime = System.currentTimeMillis();
        hotelList.stream()
            .map(HotelDoc::new).forEach(
                doc -> {
                    try {
                        client.index(b -> b
                            .index("hotel")
                            .id(String.valueOf(doc.getId()))
                            .document(doc)
                        );
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            );
        System.out.println("total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
    }


    @Test
    void testGettingDocument() throws IOException {
        GetResponse<HotelDoc> response = client.get(d -> d
            .index("hotel")
            .id("45845"), HotelDoc.class);
        if (response.found()) {
            HotelDoc hotel = response.source();
            System.out.println(hotel);
        } else {
            System.out.println("not found");
        }
    }

    @Test
    void testBulkIndexingDocuments() throws IOException {
        List<Hotel> hotelList = hotelService.list();
        long startTime = System.currentTimeMillis();
        BulkRequest.Builder builder = new BulkRequest.Builder();
        hotelList.stream().map(HotelDoc::new).forEach(hotelDoc -> {
            builder.operations(op -> op
                .index(idx -> idx
                    .index("hotel")
                    .id(String.valueOf(hotelDoc.getId()))
                    .document(hotelDoc)
                )
            );
        });
        BulkResponse result = client.bulk(builder.build());
        if (result.errors()) {
            System.out.println("Bulk had errors");
            for (BulkResponseItem item : result.items()) {
                if (item.error() != null) {
                    System.out.println(item.error().reason());
                }
            }
        }
        System.out.println("total elapsed time: " + (System.currentTimeMillis() - startTime) + "ms");
    }
}
