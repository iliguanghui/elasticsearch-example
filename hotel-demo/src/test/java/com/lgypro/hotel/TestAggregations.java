package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.DistanceUnit;
import co.elastic.clients.elasticsearch._types.GeoDistanceType;
import co.elastic.clients.elasticsearch._types.ScriptLanguage;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate;
import co.elastic.clients.elasticsearch._types.aggregations.StatsAggregate;
import co.elastic.clients.elasticsearch._types.mapping.RuntimeFieldType;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.NamedValue;
import com.lgypro.hotel.pojo.HotelDoc;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class TestAggregations {

    @Autowired
    private ElasticsearchClient client;

    @Nested
    class TestBucketAggregations {

        @Test
        void testTerms() throws IOException {
            @SuppressWarnings("unchecked")
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .range(b3 -> b3
                            .field("price")
                            .lte(JsonData.of(200))
                        )
                    )
                    .size(0)
                    .trackTotalHits(b2 -> b2
                        .enabled(false)
                    )
                    .aggregations("brand-agg",
                        b2 -> b2
                            .terms(b3 -> b3
                                .field("brand")
                                .size(50)
                                .order(NamedValue.of("_count", SortOrder.Asc))
                            )
                    )
                    .aggregations("city-agg", b2 -> b2
                        .terms(b3 -> b3
                            .field("city")
                            .size(10)
                            .order(NamedValue.of("_count", SortOrder.Desc))
                        )
                    )
                    .aggregations("price-avg", b2 -> b2
                        .avg(b3 -> b3
                            .field("price")
                        )
                    )
                , HotelDoc.class
            );
            System.out.println("-- brand-agg: ");
            Aggregate brandAgg = response.aggregations().get("brand-agg");
            brandAgg
                .sterms()
                .buckets()
                .array()
                .forEach(bucket -> {
                    String brand = bucket.key().stringValue();
                    long docCount = bucket.docCount();
                    System.out.println(brand + " = " + docCount);
                });
            System.out.println("-- city-agg");
            response.aggregations().get("city-agg")
                .sterms()
                .buckets()
                .array()
                .forEach(bucket -> {
                    String city = bucket.key().stringValue();
                    long docCount = bucket.docCount();
                    System.out.println(city + " = " + docCount);
                });
            System.out.println("-- average price");
            System.out.println("average price is " + response.aggregations().get("price-avg").avg().value());
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "aggregations": {
         *     "price-avg": {
         *       "avg": {
         *         "field": "price"
         *       }
         *     },
         *     "city-agg": {
         *       "aggregations": {
         *         "price-avg": {
         *           "avg": {
         *             "field": "price"
         *           }
         *         },
         *         "brand-agg": {
         *           "aggregations": {
         *             "price-avg": {
         *               "avg": {
         *                 "field": "price"
         *               }
         *             }
         *           },
         *           "terms": {
         *             "field": "brand",
         *             "size": 50
         *           }
         *         }
         *       },
         *       "terms": {
         *         "field": "city"
         *       }
         *     }
         *   },
         *   "size": 0
         * }
         * </pre>
         */
        @Test
        void testSubAggregations() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .size(0)
                    .trackTotalHits(b2 -> b2
                        .enabled(true)
                    )
                    .aggregations("city-agg", b2 -> b2
                        .terms(b3 -> b3
                            .field("city")
                        )
                        .meta("description", JsonData.of("one bucket per city, calculate average price"))
                        .aggregations("brand-agg", b3 -> b3
                            .terms(b4 -> b4
                                .field("brand")
                                .size(50)
                            )
                            .meta("description", JsonData.of("one bucket per city per brand, calculate average price"))
                            .aggregations("price-avg", b4 -> b4
                                .avg(b5 -> b5
                                    .field("price")
                                )
                            )
                        )
                        .aggregations("price-avg", b3 -> b3
                            .avg(b4 -> b4
                                .field("price")
                            )
                        )
                    )
                    .aggregations("price-avg", b2 -> b2
                        .meta("description", JsonData.of("average price of all hotels"))
                        .avg(b3 -> b3
                            .field("price")
                        )
                    )
                , HotelDoc.class
            );
            long totalDocs = response.hits().total().value();
            double totalAveragePrice = response.aggregations().get("price-avg").avg().value();
            System.out.printf("总共有%d所酒店，平均价格是%.2f%n", totalDocs, totalAveragePrice);
            response.aggregations().get("city-agg").sterms().buckets().array().forEach(cityBucket -> {
                String city = cityBucket.key().stringValue();
                long cityDocCount = cityBucket.docCount();
                double cityAveragePrice = cityBucket.aggregations().get("price-avg").avg().value();
                System.out.printf("\t%s总共有%d所酒店，平均价格是%.2f\n", city, cityDocCount, cityAveragePrice);
                cityBucket.aggregations().get("brand-agg").sterms().buckets().array().forEach(brandBucket -> {
                    String brand = brandBucket.key().stringValue();
                    long brandDocCount = brandBucket.docCount();
                    double cityBrandAveragePrice = brandBucket.aggregations().get("price-avg").avg().value();
                    System.out.printf("\t\t%s总共有%d所[%s]酒店，平均价格是%.2f%n", city, brandDocCount, brand, cityBrandAveragePrice);
                });
            });
        }

        @Test
        void testGeoDistance() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .size(0)
                    .aggregations("rings_around_me", b2 -> b2
                        .geoDistance(b3 -> b3
                            .field("location")
                            .origin(b4 -> b4
                                .latlon(b5 -> b5
                                    .lat(30.202131)
                                    .lon(120.2018)
                                )
                            )
                            .distanceType(GeoDistanceType.Arc)
                            .unit(DistanceUnit.Kilometers)
                            .ranges(b4 -> b4
                                .from(String.valueOf(0))
                                .to(String.valueOf(100))
                                .key("first_ring")
                            )
                            .ranges(b4 -> b4
                                .from(String.valueOf(100))
                                .to(String.valueOf(300))
                                .key("second_ring")
                            )
                            .ranges(b4 -> b4
                                .from(String.valueOf(300))
                                .to(String.valueOf(1000))
                                .key("third_ring")
                            )
                            .ranges(b4 -> b4
                                .from(String.valueOf(1000))
                                .to(String.valueOf(10000))
                                .key("fourth_ring")
                            )
                        )
                    )
                , HotelDoc.class
            );
            response.aggregations()
                .get("rings_around_me")
                .geoDistance()
                .buckets()
                .array()
                .forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "term": {
         *       "brand": {
         *         "value": "7天酒店"
         *       }
         *     }
         *   },
         *   "size": 0,
         *   "aggs": {
         *     "all_hotels": {
         *       "global": {},
         *       "aggs": {
         *         "avg_price": {
         *           "avg": {
         *             "field": "price"
         *           }
         *         }
         *       }
         *     },
         *     "single_brand": {
         *       "avg": {
         *         "field": "price"
         *       }
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testGlobal() throws IOException {
            String brand = "7天酒店";
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .term(b3 -> b3
                            .field("brand")
                            .value(brand)
                        )
                    )
                    .size(0)
                    .aggregations("all_hotels", b2 -> b2
                        .global(b3 -> b3)
                        .aggregations("avg_price", b3 -> b3
                            .avg(b4 -> b4
                                .field("price")
                            )
                        )
                    )
                    .aggregations("avg_single_brand_price", b2 -> b2
                        .avg(b3 -> b3
                            .field("price")
                        )
                    )
                , HotelDoc.class
            );
            long docCount = response.aggregations().get("all_hotels").global().docCount();
            double avgPrice = response.aggregations().get(("all_hotels")).global().aggregations().get("avg_price").avg().value();
            System.out.printf("总共统计了%d家酒店，平均价格是%.2f%n", docCount, avgPrice);
            double avgSingleBrandPrice = response.aggregations().get("avg_single_brand_price").avg().value();
            System.out.printf("酒店[%s]的平均价格是%.2f%n", brand, avgSingleBrandPrice);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "runtime_mappings": {
         *     "price.usd": {
         *       "type": "double",
         *       "script": {
         *         "source": """
         *           emit(doc['price'].value / params.cny_to_usd_exchange_rate)
         *         """,
         *         "params": {
         *           "cny_to_usd_exchange_rate": 7.2
         *         }
         *       }
         *     }
         *   },
         *   "size": 0,
         *   "aggs": {
         *     "price_ranges": {
         *       "range": {
         *         "field": "price.usd",
         *         "keyed": true,
         *         "ranges": [
         *           {
         *             "from": 0,
         *             "to": 70,
         *             "key": "cheap"
         *           },
         *           {
         *             "from": 70,
         *             "to": 140,
         *             "key": "average"
         *           },
         *           {
         *             "from": 140,
         *             "to": 1400,
         *             "key": "expensive"
         *           }
         *         ]
         *       },
         *       "aggs": {
         *         "price_stats": {
         *           "stats": {
         *             "field": "price"
         *           }
         *         }
         *       }
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testRange() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .runtimeMappings("price.usd", b2 -> b2
                        .type(RuntimeFieldType.Double)
                        .script(b3 -> b3
                            .inline(b4 -> b4
                                .lang(ScriptLanguage.Painless)
                                .source("emit(doc['price'].value / params.cny_to_usd_exchange_rate)")
                                .params("cny_to_usd_exchange_rate", JsonData.of(7.2))
                            )
                        )
                    )
                    .size(0)
                    .aggregations("price_ranges", b2 -> b2
                        .range(b3 -> b3
                            .field("price.usd")
                            .keyed(true)
                            .ranges(b4 -> b4
                                .from(String.valueOf(0))
                                .to(String.valueOf(70))
                                .key("cheap")
                            )
                            .ranges(b4 -> b4
                                .from(String.valueOf(70))
                                .to(String.valueOf(140))
                                .key("average")
                            )
                            .ranges(b4 -> b4
                                .from(String.valueOf(140))
                                .to(String.valueOf(1400))
                                .key("expensive")
                            )
                        )
                        .aggregations("price_stats", b3 -> b3
                            .stats(b4 -> b4
                                .field("price")
                            )
                        )
                    )
                , HotelDoc.class);
            response.aggregations().get("price_ranges").range().buckets().keyed().forEach((key, bucket) -> {
                Double from = bucket.from();
                Double to = bucket.to();
                long docCount = bucket.docCount();
                StatsAggregate stats = bucket.aggregations().get("price_stats").stats();
                double min = stats.min();
                double max = stats.max();
                double avg = stats.avg();
                double sum = stats.sum();
                System.out.printf("category %s: from %.2f to %.2f, count = %d, min = %.2f, max = %.2f, avg = %.2f, sum = %.2f%n",
                    key,
                    from,
                    to,
                    docCount,
                    min,
                    max,
                    avg,
                    sum
                );
            });
        }

        @Test
        void testHistogram() throws IOException {
            double interval = 50.0;
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .size(0)
                    .aggregations("price_histogram", b2 -> b2
                        .histogram(b3 -> b3
                            .field("price")
                            .interval(interval)
                            .offset(0.0)
                            .minDocCount(0)
                            .extendedBounds(b4 -> b4
                                .min(0.0)
                                .max(3500.0)
                            )
                        )
                        .aggregations("price_stats", b3 -> b3
                            .stats(b4 -> b4
                                .field("price")
                            )
                        )
                    )
                , HotelDoc.class
            );
            response.aggregations().get("price_histogram").histogram().buckets().array().forEach(bucket -> {
                double key = bucket.key();
                long docCount = bucket.docCount();
                if (docCount != 0) {
                    StatsAggregate stats = bucket.aggregations().get("price_stats").stats();
                    System.out.printf("price in [%.0f, %.0f): count = %d, min = %.0f, max = %.0f, avg = %.0f%n",
                        key,
                        key + interval,
                        docCount,
                        stats.min(),
                        stats.max(),
                        stats.avg()
                    );
                } else {
                    System.out.printf("price in [%.0f, %.0f): count = %d%n",
                        key,
                        key + interval,
                        docCount);
                }
            });
        }
    }

    @Nested
    class TestMetricsAggregations {
        @Test
        void testAvg() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .runtimeMappings("score.corrected", b2 -> b2
                        .type(RuntimeFieldType.Double)
                        .script(b3 -> b3
                            .inline(b4 -> b4
                                .source("emit(Math.min(100, doc['score'].value * params.correction))")
                                .params("correction", JsonData.of(1.2))
                            )
                        )
                    )
                    .size(0)
                    .aggregations("avg_corrected_score", b2 -> b2
                        .avg(b3 -> b3
                            .field("score.corrected")
                            .missing(1.0)
                        )
                    )
                , HotelDoc.class
            );
            System.out.printf("average score is %.2f%n", response.aggregations().get("avg_corrected_score").avg().value());
        }
    }
}
