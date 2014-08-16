package net.kuujo.copycat.vertx.util;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;

import com.google.common.util.concurrent.SettableFuture;

/**
 * Futures helpers.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class VertxFutures {

  public static <T> Handler<AsyncResult<T>> futureToHandler(final SettableFuture<T> future) {
    return new Handler<AsyncResult<T>>() {
      @Override
      public void handle(AsyncResult<T> result) {
        if (result.succeeded()) {
          future.set(result.result());
        } else {
          future.setException(result.cause());
        }
      }
    };
  }

}
