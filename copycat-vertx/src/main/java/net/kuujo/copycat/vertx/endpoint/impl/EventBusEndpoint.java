/*
 * Copyright 2014 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.kuujo.copycat.vertx.endpoint.impl;

import java.util.List;
import java.util.Map;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.uri.UriPath;
import net.kuujo.copycat.vertx.util.VertxFutures;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Event bus endpoint implementation.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusEndpoint implements Endpoint {
  private String address;
  private CopyCatContext context;
  private String host = "localhost";
  private int port;
  private Vertx vertx;

  public EventBusEndpoint() {
  }

  public EventBusEndpoint(String host, int port, String address) {
    this.host = host;
    this.port = port;
    this.address = address;
  }

  public EventBusEndpoint(Vertx vertx, String address) {
    this.vertx = vertx;
    this.address = address;
  }

  private final Handler<Message<JsonObject>> messageHandler = new Handler<Message<JsonObject>>() {
    @Override
    public void handle(final Message<JsonObject> message) {
      String command = message.body().getString("command");
      if (command != null) {
        JsonArray args = message.body().getArray("args");
        if (args == null) {
          args = new JsonArray();
        }

        Futures.addCallback(context.submitCommand(command, args.toArray()), new FutureCallback<Object>() {
          @Override
          public void onFailure(Throwable t) {
            
          }
          @Override
          @SuppressWarnings({"unchecked", "rawtypes"})
          public void onSuccess(Object result) {
            if (result instanceof Map) {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putObject("result", new JsonObject((Map) result)));
            } else if (result instanceof List) {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putArray("result", new JsonArray((List) result)));
            } else {
              message.reply(new JsonObject().putString("status", "ok").putString("leader", context.leader()).putValue("result", result));
            }
          }
        });
      }
    }
  };

  public void setContext(CopyCatContext context) {
    this.context = context;
  }

  public CopyCatContext getContext() {
    return context;
  }

  /**
   * Sets the event bus address.
   *
   * @param address The event bus address.
   */
  @UriPath
  public void setAddress(String address) {
    this.address = address;
  }

  /**
   * Returns the event bus address.
   *
   * @return The event bus address.
   */
  public String getAddress() {
    return address;
  }

  /**
   * Sets the event bus address, returning the endpoint for method chaining.
   *
   * @param address The event bus address.
   * @return The event bus endpoint.
   */
  public EventBusEndpoint withAddress(String address) {
    this.address = address;
    return this;
  }

  @Override
  public ListenableFuture<Void> start() {
    final SettableFuture<Void> future = SettableFuture.create();
    if (vertx == null) {
      vertx = new DefaultVertx(port >= 0 ? port : 0, host, new Handler<AsyncResult<Vertx>>() {
        @Override
        public void handle(AsyncResult<Vertx> result) {
          vertx.eventBus().registerHandler(address, messageHandler, VertxFutures.futureToHandler(future));
        }
      });
    } else {
      vertx.eventBus().registerHandler(address, messageHandler, VertxFutures.futureToHandler(future));
    }
    return future;
  }

  @Override
  public ListenableFuture<Void> stop() {
    final SettableFuture<Void> future = SettableFuture.create();
    vertx.eventBus().unregisterHandler(address, messageHandler, VertxFutures.futureToHandler(future));
    return future;
  }

}
