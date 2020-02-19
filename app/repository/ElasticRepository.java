package repository;

import com.fasterxml.jackson.databind.JsonNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
        //return CompletableFuture.completedFuture(new PaginatedResults<>(3, 1, 1, Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan())));
        String searched = input.trim();
        int start = size * (page - 1);
        System.out.println(start);
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{\n" +
                        "  \"query\": {\n" +
                        "    \"query_string\": {\n" +
                        "      \"fields\": [\"name^3\", \"aliases^2\", \"secretIdentity^3\"],\n" +
                        "      \"query\": \"" + searched + "\"\n" +
                        "    }\n" +
                        "  },\n" +
                        "  \"from\": "+start+"\n" +
                        "}"))
                .thenApply(response -> {
                    JsonNode responseObj = response.asJson().get("hits");
                    Iterator<JsonNode> hits = responseObj.withArray("hits").elements();
                    ArrayList<SearchedHero> heroes = new ArrayList<>();
                    while (hits.hasNext()) {
                        JsonNode hero = hits.next().get("_source");
                        SearchedHero searchedHero = SearchedHero.fromJson(hero);
                        heroes.add(searchedHero);
                    }
                    int numHeroes = responseObj.get("total").get("value").asInt();
                    int totalPage = (int) Math.ceil((double) numHeroes / size);
                    return new PaginatedResults<>(numHeroes, page, totalPage, heroes);
                });
    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        //return CompletableFuture.completedFuture(Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan()));
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{\n" +
                        "  \"suggest\": {\n" +
                        "        \"hero-suggest\" : {\n" +
                        "            \"prefix\" : \"" + input + "\", \n" +
                        "            \"completion\" : { \n" +
                        "                \"field\" : \"suggest\" \n" +
                        "            }\n" +
                        "        }\n" +
                        "    }\n" +
                        "}"))
                .thenApply(response -> {
                    JsonNode responseObj = response.asJson().get("suggest").withArray("hero-suggest").get(0);
                    Iterator<JsonNode> hits = responseObj.withArray("options").elements();
                    ArrayList<SearchedHero> heroes = new ArrayList<>();
                    while (hits.hasNext()) {
                        JsonNode hero = hits.next().get("_source");
                        SearchedHero searchedHero = SearchedHero.fromJson(hero);
                        heroes.add(searchedHero);
                    }
                    return heroes;
                });
    }
}
