package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.ScriptLanguage;
import co.elastic.clients.elasticsearch._types.mapping.RuntimeFieldType;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.elasticsearch.core.IndexResponse;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsResponse;
import co.elastic.clients.elasticsearch.indices.PutMappingResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Map;

@SpringBootTest
@Disabled
class HotelIndexTest {

    @Autowired
    private ElasticsearchClient client;

    @Test
    void contextLoaded() {

    }

    @Test
    void testCreatingIndex() throws IOException {
        boolean result = client.indices().create(b1 -> b1
            .index("hotel")
            .settings(b2 -> b2
                .numberOfReplicas(String.valueOf(0))
                .numberOfShards(String.valueOf(5))
            )
            .mappings(b2 -> b2
                .properties("id", b3 -> b3
                    .keyword(b4 -> b4)
                )
                .properties("name", b3 -> b3
                    .text(b4 -> b4
                        .analyzer("ik_max_word")
                        .copyTo("all")
                        .fields("raw", b5 -> b5
                            .keyword(b6 -> b6)
                        )
                    )
                )
                .properties("address", b3 -> b3
                    .text(b4 -> b4
                        .analyzer("ik_max_word")
                        .copyTo("all")
                        .fields("raw", b5 -> b5
                            .keyword(b6 -> b6)
                        )
                    )
                )
                .properties("price", b3 -> b3
                    .integer(b4 -> b4)
                )
                .properties("score", b3 -> b3
                    .integer(b4 -> b4)
                )
                .properties("brand", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("city", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("starName", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("business", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("location", b3 -> b3
                    .geoPoint(b4 -> b4)
                )
                .properties("pic", b3 -> b3
                    .keyword(b4 -> b4
                        .index(false)
                    )
                )
                .properties("all", b3 -> b3
                    .text(b4 -> b4
                        .analyzer("ik_max_word")
                    )
                )
            )
        ).acknowledged();
        Assertions.assertTrue(result);
    }

    @Test
    void testCreatingIndexWithPinyinAnalyzer() throws IOException {
        boolean result = client.indices().create(b1 -> b1
            .index("hotel")
            .settings(b2 -> b2
                .numberOfReplicas(String.valueOf(0))
                .numberOfShards(String.valueOf(5))
                .analysis(b3 -> b3
                    .analyzer("text_analyzer", b4 -> b4
                        .custom(b5 -> b5
                            .filter("py")
                            .tokenizer("ik_max_word")
                        )
                    )
                    .analyzer("completion_analyzer", b4 -> b4
                        .custom(b5 -> b5
                            .filter("py")
                            .tokenizer("keyword")
                        )
                    )
                    .filter("py", b4 -> b4
                        .definition(b5 -> b5
                            ._custom("pinyin", Map.of(
                                "type", "pinyin",
                                "keep_full_pinyin", false,
                                "keep_joined_full_pinyin", true,
                                "keep_original", true,
                                "limit_first_letter_length", 16,
                                "remove_duplicated_term", true,
                                "none_chinese_pinyin_tokenize", false
                            ))
                        )
                    )
                )
            )
            .mappings(b2 -> b2
                .properties("id", b3 -> b3
                    .keyword(b4 -> b4)
                )
                .properties("name", b3 -> b3
                    .text(b4 -> b4
                        .analyzer("text_analyzer")
                        .searchAnalyzer("ik_smart")
                        .copyTo("all")
                        .fields("raw", b5 -> b5
                            .keyword(b6 -> b6)
                        )
                    )
                )
                .properties("address", b3 -> b3
                    .text(b4 -> b4
                        .analyzer("ik_max_word")
                        .copyTo("all")
                        .fields("raw", b5 -> b5
                            .keyword(b6 -> b6)
                        )
                    )
                )
                .properties("price", b3 -> b3
                    .integer(b4 -> b4)
                )
                .properties("score", b3 -> b3
                    .integer(b4 -> b4)
                )
                .properties("brand", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("city", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("starName", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("business", b3 -> b3
                    .keyword(b4 -> b4
                        .copyTo("all")
                    )
                )
                .properties("location", b3 -> b3
                    .geoPoint(b4 -> b4)
                )
                .properties("pic", b3 -> b3
                    .keyword(b4 -> b4
                        .index(false)
                    )
                )
                .properties("all", b3 -> b3
                    .text(b4 -> b4
                        .analyzer("text_analyzer")
                        .searchAnalyzer("ik_max_word")
                    )
                )
                .properties("suggestion", b3 -> b3
                    .completion(b4 -> b4
                        .analyzer("completion_analyzer")
                        .searchAnalyzer("ik_smart")
                    )
                )
            )
        ).acknowledged();
        Assertions.assertTrue(result);
    }

    @Test
    public void testGetIndexSettings() throws IOException {
        GetIndicesSettingsResponse response = client.indices().getSettings(b1 -> b1
            .index("hotel")
        );
        System.out.println(response);
    }

    /**
     * <pre>
     * PUT /hotel/_mapping
     * {
     *   "runtime": {
     *     "priceLevel": {
     *       "type": "long",
     *       "script": {
     *         "source":
     *         """emit(doc['price'].value / 100)"""
     *       }
     *     }
     *   }
     * }
     * </pre>
     */
    @Test
    void testPutRuntimeFields() throws IOException {
        PutMappingResponse response = client.indices().putMapping(b1 -> b1
            .index("hotel")
            .runtime("priceLevel", b2 -> b2
                .type(RuntimeFieldType.Long)
                .script(b3 -> b3
                    .inline(b4 -> b4
                        .lang(ScriptLanguage.Painless)
                        .source("emit(doc['price'].value / 100)")
                    )
                )
            )
        );
        Assertions.assertTrue(response.acknowledged());
    }

    @Test
    void testDeletingIndex() throws IOException {
        client.indices().delete(c -> c
            .index("hotel")
        );
    }

    @Test
    void testIfIndexExists() throws IOException {
        boolean doestExists = client.indices().exists(r -> r.index("hotel")).value();
        System.out.println(doestExists);
    }

    @Test
    void testIndexingDocument() throws IOException {
        IndexResponse response = client.index(i -> i
            .index("students")
            .id("1")
            .document(Map.of(
                "name", "zhang san",
                "age", 18,
                "address", "henan"
            ))
        );
        System.out.println(response.version());
    }

    @Test
    void testGettingDocument() throws IOException {
        @SuppressWarnings("rawtypes")
        GetResponse<Map> response = client.get(d -> d
            .index("students")
            .id("1"), Map.class);
        if (response.found()) {
            @SuppressWarnings("unchecked")
            Map<String, String> student = response.source();
            System.out.println(student);
        } else {
            System.out.println("not found");
        }
    }

    @Test
    void testUpdatingDocument() throws IOException {
        client.update(u -> u
                .index("students")
                .id("1")
                .doc(Map.of(
                    "isFemale", false
                )),
            Map.class);
    }

    @Test
    void testSearchingDocuments() throws IOException {
        @SuppressWarnings("rawtypes")
        SearchResponse<Map> response = client.search(s -> s
                .index("students")
                .query(q -> q
                    .match(t -> t
                        .field("name")
                        .query("zhang")
                    )
                ),
            Map.class);
        for (Hit<Map> hit : response.hits().hits()) {
            System.out.println(hit.source());
        }
    }

    @Test
    void testDeletingDocument() throws IOException {
        client.delete(d -> d.index("students").id("1"));
    }
}
