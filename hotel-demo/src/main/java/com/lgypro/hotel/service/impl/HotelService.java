package com.lgypro.hotel.service.impl;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.*;
import co.elastic.clients.elasticsearch._types.aggregations.StringTermsBucket;
import co.elastic.clients.elasticsearch._types.query_dsl.FunctionBoostMode;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.CompletionSuggestOption;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.Suggestion;
import co.elastic.clients.json.JsonData;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lgypro.hotel.mapper.HotelMapper;
import com.lgypro.hotel.pojo.Hotel;
import com.lgypro.hotel.pojo.HotelDoc;
import com.lgypro.hotel.pojo.PageResult;
import com.lgypro.hotel.pojo.RequestParams;
import com.lgypro.hotel.service.IHotelService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class HotelService extends ServiceImpl<HotelMapper, Hotel> implements IHotelService {

    @Autowired
    private ElasticsearchClient client;

    @Override
    public PageResult search(RequestParams params) {
        try {
            Query query = buildBasicQuery(params);
            SortOptions sortOptions;
            if (params.getLocation() != null && !params.getLocation().isEmpty()) {
                String[] latlon = params.getLocation().split(",");
                sortOptions = SortOptions.of(
                    b2 -> b2
                        .geoDistance(b3 -> b3
                            .field("location")
                            .distanceType(GeoDistanceType.Arc)
                            .order(SortOrder.Asc)
                            .unit(DistanceUnit.Kilometers)
                            .location(b4 -> b4
                                .latlon(b5 -> b5
                                    .lat(Double.parseDouble(latlon[0].trim()))
                                    .lon(Double.parseDouble(latlon[1].trim()))
                                )
                            )
                        )
                );
            } else {
                sortOptions = null;
            }

            SearchRequest request =
                SearchRequest.of(b1 -> {
                    b1.index("hotel")
                        .query(b2 -> b2
                            .functionScore(b3 -> b3
                                .query(query)
                                .functions(b4 -> b4
                                    .filter(b5 -> b5
                                        .term(b6 -> b6
                                            .field("isAD")
                                            .value(true)
                                        )
                                    )
                                    .weight(10.0)
                                )
                                .boostMode(FunctionBoostMode.Sum)
                            )
                        )
                        .from((params.getPage() - 1) * params.getSize())
                        .size(params.getSize());
                    if (sortOptions != null) {
                        b1.sort(sortOptions);
                    }
                    if (params.getKey() != null && !params.getKey().isEmpty()) {
                        b1.highlight(b2 -> b2
                            .requireFieldMatch(false)
                            .fields("name", b3 -> b3)
                        );
                    }
                    return b1;
                });
            SearchResponse<HotelDoc> response = client.search(request, HotelDoc.class);
            long total = response.hits().total().value();
            List<HotelDoc> hotels = response.hits().hits().stream()
                .map(h -> {
                    if (params.getLocation() != null && !params.getLocation().isEmpty()) {
                        double distance = h.sort().get(0).doubleValue();
                        h.source().setDistance(distance);
                    }
                    if (params.getKey() != null && !params.getKey().isEmpty()) {
                        h.highlight().forEach((key, value) -> {
                            if (key.equals("name")) {
                                h.source().setName(value.get(0));
                            }
                        });
                    }
                    return h;
                })
                .map(Hit::source).toList();
            return new PageResult(total, hotels);
        } catch (
            IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Query buildBasicQuery(RequestParams params) {
        Query query = new Query.Builder()
            .bool(b2 -> {
                b2.must(b3 -> {
                        String key = params.getKey();
                        if (key == null || key.isEmpty()) {
                            b3.matchAll(b4 -> b4);
                        } else {
                            b3.match(b4 -> b4
                                .field("all")
                                .query(key)
                            );
                        }
                        return b3;
                    }
                );
                if (params.getCity() != null && !params.getCity().isEmpty()) {
                    b2.filter(b3 -> b3
                        .term(b4 -> b4
                            .field("city")
                            .value(params.getCity())
                        )
                    );
                }
                if (params.getBrand() != null && !params.getBrand().isEmpty()) {
                    b2.filter(b3 -> b3
                        .term(b4 -> b4
                            .field("brand")
                            .value(params.getBrand())
                        )
                    );
                }
                if (params.getStarName() != null && !params.getStarName().isEmpty()) {
                    b2.filter(b3 -> b3
                        .term(b4 -> b4
                            .field("starName")
                            .value(params.getStarName())
                        )
                    );
                }
                if (params.getMinPrice() != null && params.getMaxPrice() != null) {
                    b2.filter(b3 -> b3
                        .range(b4 -> b4
                            .field("price")
                            .gte(JsonData.of(params.getMinPrice()))
                            .lte(JsonData.of(params.getMaxPrice()))
                        )
                    );
                }
                return b2;
            })
            .build();
        return query;
    }

    @Override
    public Map<String, List<String>> getFilters(RequestParams params) {
        Map<String, List<String>> result = new HashMap<>();
        try {
            Query query = buildBasicQuery(params);
            SearchRequest request =
                SearchRequest.of(b1 -> {
                    b1.index("hotel")
                        .query(query)
                        .aggregations("brand", b2 -> b2
                            .terms(b3 -> b3
                                .field("brand")
                                .size(10)
                            )
                        )
                        .aggregations("city", b2 -> b2
                            .terms(b3 -> b3
                                .field("city")
                                .size(10)
                            )
                        )
                        .aggregations("starName", b2 -> b2
                            .terms(b3 -> b3
                                .field("starName")
                                .size(10)
                            )
                        )
                    ;
                    return b1;
                });
            SearchResponse<HotelDoc> response = client.search(request, HotelDoc.class);
            response.aggregations().forEach((aggregationName, aggregation) -> {
                result.put(aggregationName,
                    aggregation.sterms()
                        .buckets()
                        .array()
                        .stream()
                        .map(StringTermsBucket::key)
                        .map(FieldValue::stringValue)
                        .toList());
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public List<String> getSuggestions(String key) {
        List<String> result = new ArrayList<>();
        try {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .suggest(b2 -> b2
                        .text(key)
                        .suggesters("suggestion", b3 -> b3
                            .completion(b4 -> b4
                                .field("suggestion")
                                .skipDuplicates(true)
                                .size(10)
                            )
                        )
                    )
                , HotelDoc.class
            );
            List<Suggestion<HotelDoc>> suggestions = response.suggest().get("suggestion");
            if (!suggestions.isEmpty()) {
                suggestions.get(0)
                    .completion()
                    .options()
                    .stream()
                    .map(CompletionSuggestOption::text)
                    .forEach(result::add);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public void upsertById(Long id) {
        try {
            Hotel hotel = getById(id);
            client.index(b -> b
                .index("hotel")
                .id(String.valueOf(hotel.getId()))
                .document(new HotelDoc(hotel))
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteById(Long id) {
        try {
            client.delete(d -> d.index("hotel").id(String.valueOf(id)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
