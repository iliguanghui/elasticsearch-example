package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import com.lgypro.hotel.pojo.HotelDoc;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class HotelSortTest {

    @Autowired
    private ElasticsearchClient client;

    @Test
    void testGeoDistance() throws IOException {
        SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .matchAll(b3 -> b3)
                )
                .size(500)
                .sort(b2 -> b2
                    .geoDistance(b3 -> b3
                        .field("location")
                        .location(b4 -> b4
                            .latlon(b5 -> b5
                                .lat(31.21)
                                .lon(121.5)
                            )
                        )
                        .order(SortOrder.Asc)
                        .unit(DistanceUnit.Kilometers)
                        .mode(SortMode.Min)
                        .distanceType(GeoDistanceType.Arc)
                        .ignoreUnmapped(true)
                    )
                )
                .trackScores(true)
            , HotelDoc.class
        );
        response.hits().hits().forEach(System.out::println);
    }

    @Test
    void testScriptSorting() throws IOException {
        SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .term(b3 -> b3
                        .field("city")
                        .value("深圳")
                    )
                )
                .size(500)
                .trackScores(true)
                .sort(b2 -> b2
                    .script(b3 -> b3
                        .type(ScriptSortType.Number)
                        .script(b4 -> b4
                            .inline(b5 -> b5
                                .lang(ScriptLanguage.Painless)
                                .source("-Math.log10(doc['price'].value) * params.priceFactor + doc['score'].value * params.scoreFactor")
                                .params("priceFactor", JsonData.of(1.0))
                                .params("scoreFactor", JsonData.of(0.08))
                            )
                        )
                        .order(SortOrder.Desc)
                        .mode(SortMode.Max)
                    )
                )
            , HotelDoc.class
        );
        response.hits().hits().forEach(System.out::println);
    }
}
