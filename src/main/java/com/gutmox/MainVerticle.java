package com.gutmox;

import com.gutmox.data.processor.DataProcessor;
import com.gutmox.redis.RedisDataDao;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.reactivex.core.AbstractVerticle;
import io.vertx.reactivex.core.http.HttpServer;
import io.vertx.reactivex.ext.web.Router;
import io.vertx.reactivex.ext.web.RoutingContext;
import java.util.Arrays;

public class MainVerticle extends AbstractVerticle {

	private static final String HEALTH_CHECK = "/health";
	private static final String DATA = "/data";
	private static final String EB_DATA = "/eb-data";
	private static final String ROOT = "/";

	private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
	private final String HOST = "0.0.0.0";
	private final Integer PORT = 8080;

	private final RedisDataDao redisDataDao = new RedisDataDao();

	@Override
	public Completable rxStart() {
		vertx.exceptionHandler(error -> LOGGER.info(
			error.getMessage() + error.getCause() + Arrays.toString(error.getStackTrace()) + error
				.getLocalizedMessage()));

		new DataProcessor(vertx, redisDataDao);

		return createRouter().flatMap(router -> startHttpServer(HOST, PORT, router))
			.flatMapCompletable(httpServer -> {
				LOGGER.info("HTTP server started on http://{0}:{1}", HOST, PORT.toString());
				return Completable.complete();
			}).andThen(redisDataDao.init());
	}

	private Single<Router> createRouter() {
		Router router = Router.router(vertx);
		router.get(HEALTH_CHECK).handler(this::healthCheck);
		router.get(ROOT).handler(this::getData);
		router.get(DATA).handler(this::getData);
		router.get(EB_DATA).handler(this::getDataFromEventBus);
		return Single.just(router);
	}

	private void getDataFromEventBus(RoutingContext context) {

		vertx.eventBus().rxRequest("data", context.queryParam("key").get(0)).subscribe(res -> {

			context.response().putHeader("content-type", "application/json")
				.end(new JsonObject().put("key", res).encode());
		});

	}

	private void getData(RoutingContext context) {

		redisDataDao.getKey(context.queryParam("key").get(0)).subscribe(res -> {

			context.response().putHeader("content-type", "application/json")
				.end(new JsonObject().put("key", res).encode());
		});
	}

	private void healthCheck(RoutingContext context) {
		context.response().putHeader("content-type", "application/json")
			.end(new JsonObject().put("status", "Show must go on").encode());
	}

	private Single<HttpServer> startHttpServer(String httpHost, Integer httpPort, Router router) {
		return vertx.createHttpServer().requestHandler(router).rxListen(httpPort, httpHost);
	}
}
