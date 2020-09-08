package com.gutmox.data.processor;

import com.gutmox.redis.RedisDataDao;
import io.vertx.reactivex.core.Vertx;

public class DataProcessor {

	public DataProcessor(Vertx vertx, RedisDataDao redisDataDao) {
		vertx.eventBus().consumer("data").setMaxBufferedMessages(300).toFlowable().subscribe(msg ->{
			System.out.println(msg.body());
			msg.reply(redisDataDao.getKey(msg.body().toString()));
		});
	}
}
