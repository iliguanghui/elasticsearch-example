package com.lgypro.hotel;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.CompletionSuggestOption;
import co.elastic.clients.elasticsearch.core.search.Suggestion;
import com.lgypro.hotel.pojo.HotelDoc;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.List;

@SpringBootTest
public class TestSuggestion {
    @Autowired
    private ElasticsearchClient client;

    @Test
    void testCompletion() throws IOException {
        SearchResponse<HotelDoc> response = client.search(b1 -> b1
                .index("hotel")
                .suggest(b2 -> b2
                    .text("新")
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
                .forEach(System.out::println);
        }
    }
}
