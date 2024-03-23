package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.lgypro.hotel.pojo.HotelDoc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class HostPaginationTest {

    @Autowired
    private ElasticsearchClient client;

    @Test
    void testFromAndSize() throws IOException {
        int start = 0;
        final int size = 5;
        int count = 0;
        List<HotelDoc> hotels;
        do {
            int t = start;
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .matchAll(b3 -> b3)
                    )
                    .sort(b2 -> b2
                        .field(b3 -> b3
                            .field("price")
                            .order(SortOrder.Asc)
                        )
                    )
                    .sort(b2 -> b2
                        .field(b3 -> b3
                            .field("score")
                            .order(SortOrder.Desc)
                        )
                    )
                    .from(t)
                    .size(size)
                , HotelDoc.class);
            hotels = response.hits().hits().stream().map(Hit::source).toList();
            start += hotels.size();
            count += hotels.size();
            hotels.forEach(System.out::println);
        } while (!hotels.isEmpty());
        Assertions.assertEquals(201, count);
    }


    @Test
    void testSearchAfter() throws IOException {
        List<Hit<HotelDoc>> hotels;
        Hit<HotelDoc> last = null;
        int count = 0;
        do {
            Hit<HotelDoc> t = last;
            SearchResponse<HotelDoc> response = client.search(b1 -> {
                SearchRequest.Builder tempBuilder = b1
                    .index("hotel")
                    .query(b2 -> b2
                        .matchAll(b3 -> b3)
                    )
                    .sort(b2 -> b2
                        .field(b3 -> b3
                            .field("price")
                            .order(SortOrder.Asc)
                        )
                    )
                    .sort(b2 -> b2
                        .field(b3 -> b3
                            .field("score")
                            .order(SortOrder.Desc)
                        )
                    )
                    .size(5);
                if (t != null) {
                    tempBuilder.searchAfter(t.sort());
                }
                return tempBuilder;
            }, HotelDoc.class);
            hotels = response.hits().hits();
            count += hotels.size();
            hotels.stream().map(Hit::source).forEach(System.out::println);
            if (!hotels.isEmpty()) {
                last = hotels.get(hotels.size() - 1);
            }
        } while (!hotels.isEmpty());
        Assertions.assertEquals(201, count);
    }

    @Test
    void testSearchAfterWithPIT() throws IOException {
        int count = 0;
        String latestPitId;
        latestPitId = client.openPointInTime(b1 -> b1
            .index("hotel")
            .keepAlive(b2 -> b2
                .time("1m")
            )
        ).id();
        List<Hit<HotelDoc>> hotels;
        Hit<HotelDoc> last = null;
        do {
            Hit<HotelDoc> t1 = last;
            String t2 = latestPitId;
            SearchResponse<HotelDoc> response = client.search(b1 -> {
                SearchRequest.Builder tempBuilder = b1
                    .query(b2 -> b2
                        .matchAll(b3 -> b3)
                    )
                    .trackTotalHits(b2 -> b2
                        .enabled(false)
                    )
                    .sort(b2 -> b2
                        .field(b3 -> b3
                            .field("price")
                            .order(SortOrder.Asc)
                        )
                    )
                    .sort(b2 -> b2
                        .field(b3 -> b3
                            .field("score")
                            .order(SortOrder.Desc)
                        )
                    )
                    .sort(b2 -> b2
                        .field(b3 -> b3
                            .field("_shard_doc")
                            .order(SortOrder.Desc)
                        )
                    )
                    .pit(b2 -> b2
                        .id(t2)
                        .keepAlive(b3 -> b3
                            .time("1m")
                        )
                    )
                    .size(5);
                if (t1 != null) {
                    tempBuilder.searchAfter(t1.sort());
                }
                return tempBuilder;
            }, HotelDoc.class);
            latestPitId = response.pitId();
            hotels = response.hits().hits();
            count += hotels.size();
            hotels.stream().map(Hit::source).forEach(System.out::println);
            if (!hotels.isEmpty()) {
                last = hotels.get(hotels.size() - 1);
            }
        } while (!hotels.isEmpty());

        String t = latestPitId;
        client.closePointInTime(b1 -> b1
            .id(t)
        );
        Assertions.assertEquals(201, count);
    }
}
