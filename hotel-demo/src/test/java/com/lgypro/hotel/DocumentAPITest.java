package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Conflicts;
import co.elastic.clients.elasticsearch._types.Refresh;
import co.elastic.clients.elasticsearch._types.ScriptLanguage;
import co.elastic.clients.json.JsonData;
import com.lgypro.hotel.pojo.HotelDoc;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

@SpringBootTest
@Disabled
public class DocumentAPITest {

    @Autowired
    private ElasticsearchClient client;

    @Test
    void testDeleteByQuery() throws IOException {
        client.deleteByQuery(b1 -> b1
            .index("hotel")
            .conflicts(Conflicts.Proceed)
            .query(b2 -> b2
                .bool(b3 -> b3
                    .should(b4 -> b4
                        .match(b5 -> b5
                            .field("address")
                            .query("朝阳")
                        )
                    )
                    .should(b4 -> b4
                        .term(b5 -> b5
                            .field("city")
                            .value("上海")
                        )
                    )
                )
            )
            .refresh(true)
        );
    }

    @Nested
    class UpdateTest {
        @Test
        void testIncrementScoreWithScript() throws IOException {
            Integer score = Objects.requireNonNull(client.get(b1 -> b1
                    .index("hotel")
                    .id(String.valueOf(517915))
                , HotelDoc.class
            ).source()).getScore();
            client.update(b1 -> b1
                    .index("hotel")
                    .id(String.valueOf(517915))
                    .script(b2 -> b2
                        .inline(b3 -> b3
                            .lang(ScriptLanguage.Painless)
                            .source("ctx._source.score += params.score")
                            .params("score", JsonData.of(4))
                        )
                    )
                    .refresh(Refresh.WaitFor)
                , Map.class
            );
            Integer newScore = Objects.requireNonNull(client.get(b1 -> b1
                    .index("hotel")
                    .id(String.valueOf(517915))
                , HotelDoc.class
            ).source()).getScore();
            Assertions.assertEquals(score + 4, newScore);
        }

        @Test
        void addNewFieldToDocument() throws IOException {
            client.update(b1 -> b1
                    .index("hotel")
                    .id(String.valueOf(517915))
                    .script(b2 -> b2
                        .inline(b3 -> b3
                            .lang(ScriptLanguage.Painless)
                            .params("tags", JsonData.of(Arrays.asList("高端", "大气", "上档次")))
                            .source("ctx._source.tags = params.tags")
                        )
                    )
                , Map.class
            );
        }

        @Test
        void addValueToExistingField() throws IOException {
            client.update(b1 -> b1
                    .index("hotel")
                    .id(String.valueOf(517915))
                    .script(b2 -> b2
                        .inline(b3 -> b3
                            .lang(ScriptLanguage.Painless)
                            .params("tag", JsonData.of("奢华"))
                            .source("ctx._source.tags.add(params.tag)")
                        )
                    )
                , Map.class
            );
        }

        @Test
        void removeValueFromExistingField() throws IOException {
            client.update(b1 -> b1
                    .index("hotel")
                    .id(String.valueOf(517915))
                    .script(b2 -> b2
                        .inline(b3 -> b3
                            .lang(ScriptLanguage.Painless)
                            .params("tag", JsonData.of("上档次"))
                            .source("""
                                if (ctx._source.tags.contains(params.tag)) {
                                    ctx._source.tags.remove(ctx._source.tags.indexOf(params.tag))
                                }
                                """)
                        )
                    )
                , Map.class
            );
        }

        @Test
        void testChangeOperationWithinScript() throws IOException {
            client.update(b1 -> b1
                    .index("hotel")
                    .id(String.valueOf(517915))
                    .script(b2 -> b2
                        .inline(b3 -> b3
                            .lang(ScriptLanguage.Painless)
                            .params("city", JsonData.of("深圳"))
                            .source("""
                                if (ctx._source.city.contains(params.city))
                                {
                                    ctx.op = 'delete'
                                } else {
                                    ctx.op = 'noop'
                                }
                                """)
                        )
                    )
                , Map.class
            );
        }
    }

    @Test
    void testReindex() throws IOException {
        client.reindex(b1 -> b1
            .conflicts(Conflicts.Proceed)
            .source(b2 -> b2
                .index("hotel")
                .query(b3 -> b3
                    .term(b4 -> b4
                        .field("city")
                        .value("上海")
                    )
                )
            )
            .dest(b2 -> b2
                .index("hotel-reindex")
            )
        );
    }
}
