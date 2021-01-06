package com.vertx.commons.jdbc;

import java.util.Objects;

import com.vertx.commons.annotations.ServiceProvider;
import com.vertx.commons.deploy.service.RecordService;
import com.vertx.commons.deploy.service.secret.Secret;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.servicediscovery.types.JDBCDataSource;

/**
 * @author Luiz Schmidt
 */
@ServiceProvider(name = JDBCService.SERVICE)
public class JDBCService implements RecordService {

  public static final String SERVICE = "jdbc-service";

  public static Future<JDBCClient> jdbc(ServiceDiscovery discovery) {
    Promise<JDBCClient> promise = Promise.promise();

    JDBCDataSource.getJDBCClient(discovery, new JsonObject().put("name", SERVICE)).onSuccess(promise::complete)
        .onFailure(cause -> promise.fail(RecordService.buildErrorMessage(SERVICE, cause)));
    
    return promise.future();
  }

  public Future<Record> newRecord(Vertx vertx, JsonObject config) {
    Promise<Record> promise = Promise.promise();
    buildjdbcOptions(vertx, config)
        .onSuccess(opts -> promise.complete(JDBCDataSource.createRecord(SERVICE, new JsonObject(), opts)))
        .onFailure(promise::fail);
    return promise.future();
  }

  private static Future<JsonObject> buildjdbcOptions(Vertx vertx, JsonObject config) {
    Objects.requireNonNull(config.getString("url"), "'url' is required");
    Objects.requireNonNull(config.getString("driver_class"), "'drive_class' is required");
    
    Promise<JsonObject> promise = Promise.promise();

    Secret.clear(config);
    Secret.getUsernameAndPassword(vertx, config).onComplete(res -> {
      if (res.succeeded()) {
        promise.complete(config.mergeIn(res.result()));
      } else {
        promise.fail(res.cause());
      }
    });

    return promise.future();
  }
}
