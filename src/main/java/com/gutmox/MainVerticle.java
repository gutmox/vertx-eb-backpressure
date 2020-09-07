package com.gutmox;

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
	private static final String HELLO = "/hello";
	private static final String ROOT = "/";

	private static final Logger LOGGER = LoggerFactory.getLogger(MainVerticle.class.getName());
	private final String HOST = "0.0.0.0";
	private final Integer PORT = 8080;

	@Override
	public Completable rxStart() {
		vertx.exceptionHandler(error -> LOGGER.info(
			error.getMessage() + error.getCause() + Arrays.toString(error.getStackTrace()) + error
				.getLocalizedMessage()));

		return createRouter().flatMap(router -> startHttpServer(HOST, PORT, router))
			.flatMapCompletable(httpServer -> {
				LOGGER.info("HTTP server started on http://{0}:{1}", HOST, PORT.toString());
				return Completable.complete();
			});
	}

	private Single<Router> createRouter() {
		Router router = Router.router(vertx);
		router.get(HEALTH_CHECK).handler(this::healthCheck);
		router.get(HELLO).handler(this::hello);
		router.get(ROOT).handler(this::hello);
		return Single.just(router);
	}

	private void hello(RoutingContext context) {
		context.response().putHeader("content-type", "application/json")
			.end(new JsonObject().put("hello", "world").encode());
	}

	private void healthCheck(RoutingContext context) {
		context.response().putHeader("content-type", "application/json")
			.end(new JsonObject().put("status", "Show must go on").encode());
	}

	private Single<HttpServer> startHttpServer(String httpHost, Integer httpPort, Router router) {
		return vertx.createHttpServer().requestHandler(router).rxListen(httpPort, httpHost);
	}
}
