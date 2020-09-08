package com.gutmox.data.processor;

import com.gutmox.redis.RedisDataDao;
import io.reactivex.Completable;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.core.Vertx;

public class DataProcessor {

	public DataProcessor(Vertx vertx, RedisDataDao redisDataDao) {
		vertx.eventBus().consumer("data").setMaxBufferedMessages(300).toFlowable()
			.flatMapCompletable(msg -> {
				System.out.println(msg.body());
				return redisDataDao.getKey(msg.body().toString()).flatMapCompletable(rs -> {
					msg.reply(new JsonObject().put("name", rs));
					return Completable.complete();
				});
			}).subscribe();
	}
}
