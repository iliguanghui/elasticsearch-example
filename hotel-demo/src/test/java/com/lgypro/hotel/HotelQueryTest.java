package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.GeoLocation;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.JsonData;
import com.lgypro.hotel.pojo.HotelDoc;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class HotelQueryTest {

    @Autowired
    private ElasticsearchClient client;

    @Nested
    class FullTextQueries {
        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "match_all": {}
         *   }
         * }
         * </pre>
         */
        @Test
        @DisplayName("get all documents within the index")
        void testMatchAll() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b -> b
                    .index("hotel")
                    .query(q -> q
                        .matchAll(m -> m)),
                HotelDoc.class
            );
            long totalHits = response.hits().total().value();
            assertEquals(201, totalHits);
            Double maxScore = response.hits().maxScore();
            assertEquals(1.0, maxScore, 0.0001);
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        @Nested
        class TestMatch {
            /**
             * <pre>
             * GET /hotel/_search
             * {
             *   "query": {
             *     "match": {
             *       "all": {
             *         "query": "如家外滩"
             *       }
             *     }
             *   }
             * }
             * </pre>
             */
            @Test
            @DisplayName("return documents contain any terms")
            void testMatch() throws IOException {
                SearchResponse<HotelDoc> response = client.search(s -> s
                    .index("hotel")
                    .query(q -> q
                        .match(m -> m
                            .field("all")
                            .query("如家外滩")
                        )
                    ), HotelDoc.class
                );
                long totalHits = response.hits().total().value();
                assertEquals(32, totalHits);
                response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
            }

            /**
             * <pre>
             * GET /hotel/_search
             * {
             *   "query": {
             *     "match": {
             *       "all": {
             *         "query": "如家外滩",
             *         "operator": "and",
             *         "analyzer": "ik_smart"
             *       }
             *     }
             *   }
             * }
             * </pre>
             */
            @Test
            @DisplayName("if operator is and, document must contain all terms which come from query text analyzed by analyzer")
            void testMatchWithAndOperator() throws IOException {
                SearchResponse<HotelDoc> response = client.search(s -> s
                    .index("hotel")
                    .query(q -> q
                        .match(m -> m
                            .field("all")
                            .query("如家外滩")
                            .operator(Operator.And)
                            .analyzer("ik_smart")
                        )
                    ), HotelDoc.class
                );
                long totalHits = response.hits().total().value();
                assertEquals(1, totalHits);
                HotelDoc hotel = response.hits().hits().get(0).source();
                assertEquals(434082L, hotel.getId());
                System.out.println(hotel);
            }

            @Test
            void testMatchMinimumShouldMatch() throws IOException {
                SearchResponse<HotelDoc> response = client.search(s -> s
                    .index("hotel")
                    .query(q -> q
                        .match(m -> m
                            .field("all")
                            .query("如家外滩")
                            .minimumShouldMatch(String.valueOf(2))
                        )
                    ), HotelDoc.class
                );
                long totalHits = response.hits().total().value();
                assertEquals(1, totalHits);
                HotelDoc hotel = response.hits().hits().get(0).source();
                assertEquals(434082L, hotel.getId());
                System.out.println(hotel);
            }

            /**
             * <pre>
             * GET /hotel/_search
             * {
             *   "query": {
             *     "match": {
             *       "all": {
             *         "query": "to be or not to be",
             *         "analyzer": "stop",
             *         "zero_terms_query": "all"
             *       }
             *     }
             *   }
             * }
             * </pre>
             */
            @Test
            @DisplayName("return all documents if analyzer removes all tokens")
            void testMatchZeroTermsQuery() throws IOException {
                SearchResponse<HotelDoc> response = client.search(s -> s
                    .index("hotel")
                    .query(q -> q
                        .match(m -> m
                            .field("all")
                            .query("to be or not to be")
                            .analyzer("stop")
                            .zeroTermsQuery(ZeroTermsQuery.All)
                        )
                    ), HotelDoc.class
                );
                long totalHits = response.hits().total().value();
                assertEquals(201, totalHits);
            }

            @Test
            void testMatchPhrase() throws IOException {
                SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .matchPhrase(b3 -> b3
                            .field("all")
                            .query("北京颐和园酒店")
                            .slop(2)
                        )
                    ), HotelDoc.class
                );
                long totalHits = response.hits().total().value();
                assertEquals(0, totalHits);
            }
        }


        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "multi_match": {
         *       "query": "如家外滩",
         *       "fields": ["business", "city", "brand", "name"]
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testMultiMatch() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .multiMatch(m -> m
                        .query("如家外滩")
                        .fields(List.of(
                            "business",
                            "city",
                            "brand",
                            "name"
                        ))
                    )
                ), HotelDoc.class
            );
            long totalHits = response.hits().total().value();
            assertEquals(32, totalHits);
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "match_bool_prefix" : {
         *       "name" : {
         *         "query": "至尊颐",
         *         "analyzer": "ik_smart",
         *         "minimum_should_match": 2
         *       }
         *     }
         *   },
         *   "explain": true
         * }
         * </pre>
         * A match_bool_prefix query analyzes its input and constructs a bool query from the terms.
         * Each term except the last is used in a term query.
         * The last term is used in a prefix query.
         */
        @Test
        void testMatchBoolPrefix() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .matchBoolPrefix(b3 -> b3
                            .field("name")
                            .query("至尊颐")
                            .analyzer("ik_smart")
                            .minimumShouldMatch(String.valueOf(2))
                        )
                    )
                    .explain(true),
                HotelDoc.class);
            long totalHits = response.hits().total().value();
            assertEquals(1, totalHits);
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "match_phrase_prefix": {
         *       "address": {
         *         "query": "人民南路2"
         *       }
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testMatchPhrasePrefix() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .matchPhrasePrefix(b3 -> b3
                            .field("address")
                            .query("人民南路2")
                        )
                    )
                , HotelDoc.class);
            long totalHits = response.hits().total().value();
            assertEquals(1, totalHits);
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }
    }

    @Nested
    class TermLevelQueries {
        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "term": {
         *       "city": {
         *         "value": "上海"
         *       }
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testTerm() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .term(t -> t
                        .field("city")
                        .value("上海")
                    )
                ), HotelDoc.class
            );
            assertEquals(83, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        @Test
        void testRange() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .range(r -> r
                        .field("price")
                        .gte(JsonData.of(100))
                        .lte(JsonData.of(300))
                    )
                ), HotelDoc.class
            );
            assertEquals(50, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "terms": {
         *       "city": [
         *         "北京",
         *         "上海"
         *       ]
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testTerms() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .terms(t -> t
                        .field("city")
                        .terms(v -> v
                            .value(List.of(
                                FieldValue.of("上海"),
                                FieldValue.of("北京")
                            ))
                        )
                    )), HotelDoc.class
            );
            assertEquals(145, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "terms": {
         *       "city": {
         *         "index": "hotel",
         *         "id": "624417",
         *         "path": "city",
         *         "routing": "624417"
         *       }
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testTermsLookup() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .terms(t -> t
                        .field("city")
                        .terms(v -> v
                            .lookup(l -> l
                                .index("hotel")
                                .id("624417")
                                .path("city")
                                .routing("624417")
                            )
                        )
                    )), HotelDoc.class
            );
            assertEquals(56, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        /**
         * <pre>
         * POST /hotel/_search
         * {
         *   "query": {
         *     "ids": {
         *       "values": [
         *         "600001",
         *         "517915",
         *         "527938",
         *         "546869",
         *         "584697"
         *       ]
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testIds() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .ids(i -> i
                        .values(List.of(
                            "600001", /*fake*/
                            "517915",
                            "527938",
                            "546869",
                            "584697"
                        ))
                    )
                ), HotelDoc.class
            );
            assertEquals(4, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "prefix": {
         *       "brand": {
         *         "value": "7天"
         *       }
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testPrefix() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .prefix(p -> p
                        .field("brand")
                        .value("7天")
                    )
                ), HotelDoc.class
            );
            assertEquals(30, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        @Test
        void testPrefixAgainstTextField() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .prefix(p -> p
                        .field("address")
                        .value("东方")
                    )
                ), HotelDoc.class
            );
            assertEquals(4, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        @Test
        void testFindingDocumentsMissingIndexedValues() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .bool(b3 -> b3
                        .mustNot(b4 -> b4
                            .exists(b5 -> b5
                                .field("location")
                            )
                        )
                    )
                ), HotelDoc.class
            );
            assertEquals(0, response.hits().total().value());
        }

        @Test
        void testFuzzy() throws IOException {
            /*
             * 可能由于中英文差异，这里匹配不上
             */
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .fuzzy(b3 -> b3
                        .field("city")
                        .value("海上")
                    )
                ), HotelDoc.class
            );
            assertEquals(0, response.hits().total().value());
        }
    }

    @Nested
    class TestGeoQueries {
        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "bool": {
         *       "must": {
         *         "match_all": {}
         *       },
         *       "filter": {
         *         "geo_distance": {
         *           "distance": "5km",
         *           "location": {
         *             "lat": 31.21,
         *             "lon": 121.5
         *           }
         *         }
         *       }
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testGeoDistance() throws IOException {
            SearchResponse<HotelDoc> response = client.search(s -> s
                .index("hotel")
                .query(q -> q
                    .bool(b -> b
                        .must(m -> m
                            .matchAll(a -> a)
                        )
                        .filter(f -> f
                            .geoDistance(g -> g
                                .field("location")
                                .distance("5km")
                                .location(l -> l
                                    .latlon(d -> d
                                        .lat(31.21)
                                        .lon(121.5)
                                    )
                                )
                            )
                        )
                    )
                )
                .size(50), HotelDoc.class
            );
            assertEquals(13, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }
    }

    @Nested
    class TestFunctionScore {
        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "function_score": {
         *       "query": { "match_all": {} },
         *       "boost": "5",
         *       "random_score": {},
         *       "boost_mode": "multiply"
         *     }
         *   },
         *   "explain": true
         * }
         * </pre>
         */
        @Test
        void testRandomScore() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b0 -> b0
                .index("hotel")
                .query(b1 -> b1
                    .functionScore(b2 -> b2
                        .query(b3 -> b3
                            .matchAll(b4 -> b4)
                        )
                        .boost(5F)
                        .boostMode(FunctionBoostMode.Multiply)
                        .functions(b3 -> b3
                            .randomScore(b4 -> b4)
                        )
                    )
                ), HotelDoc.class
            );
            assertEquals(201, response.hits().total().value());
            response.hits().hits().stream()
                .map(h -> java.util.Map.of(
                    "score", h.score(),
                    "source", h.source()))
                .forEach(System.out::println);
        }

        /**
         * <pre>
         * POST /hotel/_explain/441836
         * {
         *   "query": {
         *     "function_score": {
         *       "query": { "match_all": {} },
         *       "boost": "5",
         *       "functions": [
         *         {
         *           "filter": { "match": { "address": "人民" } },
         *           "random_score": {},
         *           "weight": 23
         *         },
         *         {
         *           "filter": { "term": { "brand": "如家" } },
         *           "weight": 42
         *         }
         *       ],
         *       "max_boost": 42,
         *       "score_mode": "max",
         *       "boost_mode": "multiply",
         *       "min_score": 42
         *     }
         *   }
         * }
         * </pre>
         */
        @Test
        void testMultipleFunctions() throws java.io.IOException {
            SearchResponse<HotelDoc> response = client.search(b0 -> b0
                .index("hotel")
                .query(b1 -> b1
                    .functionScore(b2 -> b2
                        .query(b3 -> b3
                            .matchAll(b4 -> b4)
                        )
                        .functions(b3 -> b3
                            .filter(b4 -> b4
                                .match(b5 -> b5
                                    .field("address")
                                    .query("人民")
                                )
                            )
                            .randomScore(b5 -> b5)
                            .weight(23D)
                        )
                        .functions(b3 -> b3
                            .filter(b4 -> b4
                                .term(b5 -> b5
                                    .field("brand")
                                    .value("如家")
                                )
                            )
                            .weight(42D)
                        )
                        .boost(5F)
                        .maxBoost(42D)
                        .scoreMode(FunctionScoreMode.Max)
                        .boostMode(FunctionBoostMode.Multiply)
                        .minScore(42D)
                    )
                ), HotelDoc.class
            );
            assertEquals(30, response.hits().total().value());
            response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "function_score": {
         *       "query": {
         *         "match": {
         *           "address": {
         *             "query": "朝阳"
         *           }
         *         }
         *       },
         *       "functions": [
         *         {
         *           "filter": {
         *             "term": {
         *               "brand": {
         *                 "value": "7天酒店"
         *               }
         *             }
         *           },
         *           "script_score": {
         *             "script": "doc['price'].value * 0.015"
         *           }
         *         },
         *         {
         *           "filter": {
         *             "term": {
         *               "brand": {
         *                 "value": "如家"
         *               }
         *             }
         *           },
         *           "script_score": {
         *             "script": "doc['price'].value * 0.01"
         *           }
         *         },
         *         {
         *           "filter": {
         *             "range": {
         *               "price": {
         *                 "lte": 400
         *               }
         *             }
         *           },
         *           "weight": 1.5
         *         },
         *         {
         *           "filter": {
         *             "range": {
         *               "score": {
         *                 "gte": 30,
         *                 "lte": 40
         *               }
         *             }
         *           },
         *           "field_value_factor": {
         *             "field": "score",
         *             "factor": 0.1,
         *             "modifier": "ln2p",
         *             "missing": 35
         *           }
         *         }
         *       ],
         *       "score_mode": "sum",
         *       "boost_mode": "multiply",
         *       "max_boost": 100
         *     }
         *   },
         *   "explain": true
         * }
         * </pre>
         */
        @Test
        void testComplexScoring() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .functionScore(b3 -> b3
                        .query(b4 -> b4
                            .match(b5 -> b5
                                .field("address")
                                .query("朝阳")
                            )
                        )
                        .functions(b4 -> b4
                            .filter(b5 -> b5
                                .term(b6 -> b6
                                    .field("brand")
                                    .value("7天酒店")
                                )
                            )
                            .scriptScore(b5 -> b5
                                .script(b6 -> b6
                                    .inline(b7 -> b7
                                        .source("doc['price'].value * 0.015")
                                    )
                                )
                            )
                        )
                        .functions(b4 -> b4
                            .filter(b5 -> b5
                                .term(b6 -> b6
                                    .field("brand")
                                    .value("如家")
                                )
                            )
                            .scriptScore(b5 -> b5
                                .script(b6 -> b6
                                    .inline(b7 -> b7
                                        .source("doc['price'].value * 0.01")
                                    )
                                )
                            )
                        )
                        .functions(b4 -> b4
                            .filter(b5 -> b5
                                .range(b6 -> b6
                                    .field("price")
                                    .lte(JsonData.of(400))
                                )
                            )
                            .weight(1.5)
                        )
                        .functions(b4 -> b4
                            .filter(b5 -> b5
                                .range(b6 -> b6
                                    .field("score")
                                    .gte(JsonData.of(30))
                                    .lte(JsonData.of(40))
                                )
                            )
                            .fieldValueFactor(b5 -> b5
                                .field("score")
                                .factor(0.1)
                                .modifier(FieldValueFactorModifier.Log2p)
                                .missing(35.0)
                            )
                        )
                        .scoreMode(FunctionScoreMode.Sum)
                        .boostMode(FunctionBoostMode.Multiply)
                        .maxBoost(100.0)
                    )
                ), HotelDoc.class);
            response.hits().hits().forEach(System.out::println);
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "function_score": {
         *       "functions": [
         *         {
         *           "gauss": {
         *             "price": {
         *               "scale": 300,
         *               "origin": 0
         *             }
         *           }
         *         },
         *         {
         *           "exp": {
         *             "location": {
         *               "decay": 0.6,
         *               "scale": "2km",
         *               "origin": {
         *                 "lat": 22.54093,
         *                 "lon": 114.10858
         *               }
         *             }
         *           }
         *         },
         *         {
         *           "linear": {
         *             "score": {
         *               "scale": 30,
         *               "origin": 50
         *             }
         *           }
         *         }
         *       ],
         *       "query": {
         *         "term": {
         *           "city": {
         *             "value": "深圳"
         *           }
         *         }
         *       }
         *     }
         *   },
         *   "size": 100
         * }
         * </pre>
         */
        @Test
        void testDecayFunctions() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .functionScore(b3 -> b3
                        .query(b4 -> b4
                            .term(b5 -> b5
                                .field("city")
                                .value("深圳")
                            )
                        )
                        .functions(b4 -> b4
                            .gauss(b5 -> b5
                                .field("price")
                                .placement(b6 -> b6
                                    .origin(JsonData.of(0))
                                    .scale(JsonData.of(300))
                                )
                            )
                        )
                        .functions(b4 -> b4
                            .exp(b5 -> b5
                                .field("location")
                                .placement(b6 -> b6
                                    .origin(JsonData.of(GeoLocation.of(b7 -> b7
                                        .latlon(b8 -> b8
                                            .lat(22.54093)
                                            .lon(114.10858)
                                        )
                                    )))
                                    .scale(JsonData.of("2km"))
                                    .decay(0.6)
                                )
                            )
                        )
                        .functions(b4 -> b4
                            .linear(b5 -> b5
                                .field("score")
                                .placement(b6 -> b6
                                    .origin(JsonData.of(50))
                                    .scale(JsonData.of(30))
                                )
                            )
                        )
                    )
                )
                .size(100), HotelDoc.class);
            response.hits().hits().forEach(System.out::println);
        }
    }

    @Nested
    class TestBooleanQueries {
        @Test
        void testMust() throws IOException {
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .bool(b3 -> b3
                        .must(b4 -> b4
                            .range(b5 -> b5
                                .field("price")
                                .gte(JsonData.of(300))
                                .lte(JsonData.of(500))
                            )
                        )
                        .must(b4 -> b4
                            .prefix(b5 -> b5
                                .field("brand")
                                .value("7天")
                            )
                        )
                        /* should属于可选条件，匹配越多，得分越高 */
                        .should(b4 -> b4
                            .match(b5 -> b5
                                .field("name")
                                .query("宝山")
                            )
                        )
                        .minimumShouldMatch(String.valueOf(0))
                    )
                ), HotelDoc.class
            );
            assertEquals(10, response.hits().total().value());
        }

        /**
         * <pre>
         * GET /hotel/_search
         * {
         *   "query": {
         *     "bool": {
         *       "must_not": [
         *         {
         *           "term": {
         *             "city": {
         *               "value": "北京"
         *             }
         *           }
         *         },
         *         {
         *           "term": {
         *             "city": {
         *               "value": "深圳"
         *             }
         *           }
         *         }
         *       ],
         *       "must": [
         *         {
         *           "range": {
         *             "price": {
         *               "lte": 500
         *             }
         *           }
         *         }
         *       ],
         *       "filter": [
         *         {
         *           "terms": {
         *             "starName": [
         *               "三钻",
         *               "四钻"
         *             ]
         *           }
         *         }
         *       ],
         *       "should": [
         *         {
         *           "match": {
         *             "address": {
         *               "query": "华夏"
         *             }
         *           }
         *         }
         *       ]
         *     }
         *   },
         *   "explain": true
         * }
         * </pre>
         */
        @Test
        void testBool() throws IOException {
            /*
                select * from tb_hotel
                where city != '北京' and city != '深圳'
                and price <= 500 and star_name in ('三钻', '四钻');
             */
            SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .bool(b3 -> b3
                        .mustNot(b4 -> b4
                            .term(b5 -> b5
                                .field("city")
                                .value("北京")
                            )
                        )
                        .mustNot(b4 -> b4
                            .term(b5 -> b5
                                .field("city")
                                .value("深圳")
                            )
                        )
                        .must(b4 -> b4
                            .range(b5 -> b5
                                .field("price")
                                .lte(JsonData.of(500))
                            )
                        )
                        .filter(b4 -> b4
                            .terms(b5 -> b5
                                .field("starName")
                                .terms(b6 -> b6
                                    .value(List.of(
                                        FieldValue.of("三钻"),
                                        FieldValue.of("四钻")
                                    ))
                                )
                            )
                        )
                        .should(b4 -> b4
                            .match(b5 -> b5
                                .field("address")
                                .query("华夏")
                            )
                        )
                    )
                ), HotelDoc.class
            );
            assertEquals(5, response.hits().total().value());
            response.hits().hits().forEach(System.out::println);
        }

        @Nested
        class TestFilter {
            /**
             * <pre>
             * GET /hotel/_search
             * {
             *   "query": {
             *     "bool": {
             *       "filter": [
             *         {
             *           "term": {
             *             "city": {
             *               "value": "深圳"
             *             }
             *           }
             *         },
             *         {
             *           "range": {
             *             "price": {
             *               "gte": 300,
             *               "lte": 400
             *             }
             *           }
             *         },
             *         {
             *           "match": {
             *             "name": {
             *               "query": "汉庭"
             *             }
             *           }
             *         }
             *       ]
             *     }
             *   }
             * }
             * </pre>
             */
            @Test
            @DisplayName("queries specified under the filter element have no effect on scoring - scores are returned as 0")
            void testSimpleFilter() throws IOException {
                SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .bool(b3 -> b3
                            .filter(b4 -> b4
                                .term(b5 -> b5
                                    .queryName("city rule")
                                    .field("city")
                                    .value("深圳")
                                )
                            )
                            .filter(b4 -> b4
                                .range(b5 -> b5
                                    .queryName("price rule")
                                    .field("price")
                                    .gte(JsonData.of(300))
                                    .lte(JsonData.of(400))
                                )
                            )
                            .filter(b4 -> b4
                                .match(b5 -> b5
                                    .queryName("name rule")
                                    .field("name")
                                    .query("汉庭") /* 使用analysis-ik的自定义词典把酒店名作为一个token处理*/
                                )
                            )
                        )
                    ), HotelDoc.class
                );
                assertEquals(2, response.hits().total().value());
                assertEquals(0, response.hits().maxScore());
                response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
            }

            @Test
            @DisplayName("the bool query has a match_all query, which assigns a score of 1.0 to all documents")
            void testSimpleFilterWithMatchAll() throws IOException {
                SearchResponse<HotelDoc> response = client.search(b1 -> b1
                    .index("hotel")
                    .query(b2 -> b2
                        .bool(b3 -> b3
                            .must(b4 -> b4
                                .matchAll(b5 -> b5)
                            )
                            .filter(b4 -> b4
                                .term(b5 -> b5
                                    .queryName("city rule")
                                    .field("city")
                                    .value("深圳")
                                )
                            )
                            .filter(b4 -> b4
                                .range(b5 -> b5
                                    .queryName("price rule")
                                    .field("price")
                                    .gte(JsonData.of(300))
                                    .lte(JsonData.of(400))
                                )
                            )
                            .filter(b4 -> b4
                                .match(b5 -> b5
                                    .queryName("name rule")
                                    .field("name")
                                    .query("汉庭") /* 使用analysis-ik的自定义词典把酒店名作为一个token处理*/
                                )
                            )
                        )
                    ), HotelDoc.class
                );
                assertEquals(2, response.hits().total().value());
                assertEquals(1, response.hits().maxScore());
                response.hits().hits().stream().map(Hit::source).forEach(System.out::println);
            }
        }
    }

    @Test
    void testBoosting() throws IOException {
        SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .query(b2 -> b2
                    .boosting(b3 -> b3
                        .positive(b4 -> b4
                            .bool(b5 -> b5
                                .must(b6 -> b6
                                    .term(b7 -> b7
                                        .field("brand")
                                        .value("7天酒店")
                                    )
                                )
                                .must(b6 -> b6
                                    .match(b7 -> b7
                                        .field("name")
                                        .query("北京")
                                    )
                                )
                            )
                        )
                        .negative(b4 -> b4
                            .range(b5 -> b5
                                .field("score")
                                .lt(JsonData.of(40))
                            )
                        )
                        .negativeBoost(0.1)
                    )
                )
                .size(50)
            , HotelDoc.class
        );
        System.out.println(response.hits().total().value());
        response.hits().hits().forEach(System.out::println);
    }

    @Test
    void testConstantScore() throws IOException {
        SearchResponse<HotelDoc> response = client.search(b1 -> b1
            .index("hotel")
            .query(b2 -> b2
                .constantScore(b3 -> b3
                    .filter(b4 -> b4
                        .term(b5 -> b5
                            .field("brand")
                            .value("7天酒店")
                        )
                    )
                    .boost(1.2F)
                )
            ), HotelDoc.class);
        response.hits().hits().forEach(System.out::println);
    }

    @Test
    void testDisjunctionMax() throws IOException {
        SearchResponse<HotelDoc> response = client.search(b1 -> b1
            .index("hotel")
            .query(b2 -> b2
                .disMax(b3 -> b3
                    .tieBreaker(0.7)
                    .queries(b4 -> b4
                        .term(b5 -> b5
                            .field("brand")
                            .value("7天酒店")
                        )
                    )
                    .queries(b4 -> b4
                        .match(b5 -> b5
                            .field("name")
                            .query("北京")
                        )
                    )
                )
            ), HotelDoc.class);
        response.hits().hits().forEach(System.out::println);
    }
}
