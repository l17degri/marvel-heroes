package repository;

import com.sun.jdi.LongValue;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.api.sync.RedisCommands;
import models.StatItem;
import models.TopStatItem;
import play.Logger;

import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");
    private final RedisClient redisClient;
    private final String LAST = "lasts";
    private final String TOP = "top";

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }

    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().zincrby(TOP, 1, statItem.toJson().toString()).thenApply(res -> {
            connection.close();
            return true;
        });
    }

    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().lpush(LAST, statItem.toJson().toString()).thenApply(res -> {
            connection.close();
            return 1L;
        });
    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.async().lrange(LAST, 0, count - 1).thenApply((res -> {
            return res.stream().map(last -> StatItem.fromJson(last)).collect(Collectors.toList());
        }));
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        logger.info("Retrieved tops heroes");
        return connection.async().zrevrangeWithScores(TOP, 0, count - 1).thenApply(res -> {
            connection.close();
            return res.stream().map(hit -> new TopStatItem(StatItem.fromJson(hit.getValue()), (long) hit.getScore()))
                    .collect(Collectors.toList());
        });
    }
}
