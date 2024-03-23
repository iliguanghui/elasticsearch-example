package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import com.lgypro.hotel.pojo.HotelDoc;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class TestHighlighting {
    @Autowired
    private ElasticsearchClient client;

    @Test
    void testHighlighting() throws IOException {
        SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .bool(b3 -> b3
                        .should(b4 -> b4
                            .match(b5 -> b5
                                .field("all")
                                .query("如家")
                            )
                        )
                        .should(b4 -> b4
                            .match(b5 -> b5
                                .field("all")
                                .query("汉庭")
                            )
                        )
                    )
                )
                .size(500)
                .highlight(b2 -> b2
                    .requireFieldMatch(false)
                    .fields("name", b3 -> b3)
                )
            , HotelDoc.class
        );
        List<Hit<HotelDoc>> hits = response.hits().hits();
        hits.forEach(h -> {
            h.highlight().forEach((key, value) -> {
                if ("name".equals(key)) {
                    h.source().setName(value.get(0));
                }
            });
        });
        hits.stream().map(Hit::source).forEach(System.out::println);
    }
}
