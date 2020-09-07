package com.gutmox.redis;

import io.reactivex.Completable;
import io.reactivex.Maybe;
import io.reactivex.Single;
import io.vertx.reactivex.core.Vertx;
import io.vertx.reactivex.redis.client.Redis;
import io.vertx.reactivex.redis.client.RedisAPI;
import io.vertx.reactivex.redis.client.Response;
import io.vertx.redis.client.RedisOptions;
import java.util.concurrent.TimeUnit;

public class RedisDataDao {

	private RedisAPI redisAPI;

	public Completable init() {

		return Redis.createClient(Vertx.currentContext().owner(), new RedisOptions()
			.setConnectionString("redis://localhost:6379")
			.setMaxPoolSize(20)
			.setPoolCleanerInterval(100)
			.setMaxPoolWaiting(1000)
			.setMaxWaitingHandlers(512)).rxConnect()
			.map(connection -> {
				connection.exceptionHandler(Throwable::printStackTrace);
				return RedisAPI.api(connection);
			})
			.flatMapCompletable(res -> {
				redisAPI = res;
				return Completable.complete();
			}).doOnError(Throwable::printStackTrace);

	}

	public Single<String> getKey(String key){

		return redisAPI.rxGet(key)
			.timeout(60, TimeUnit.SECONDS)
			.map(Response::toString)
			.switchIfEmpty(Maybe.just("{}"))
			.toSingle()
			.doOnError(Throwable::printStackTrace);

	}
}
