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
package net.kuujo.copycat.vertx.protocol.impl;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Vert.x event bus protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class EventBusProtocolClient implements ProtocolClient {
  private final String address;
  private final String host;
  private final int port;
  private Vertx vertx;

  public EventBusProtocolClient(String address, String host, int port) {
    this.address = address;
    this.host = host;
    this.port = port;
  }

  public EventBusProtocolClient(String address, Vertx vertx) {
    this.address = address;
    this.host = null;
    this.port = 0;
    this.vertx = vertx;
  }

  @Override
  public ListenableFuture<AppendEntriesResponse> appendEntries(final AppendEntriesRequest request) {
    final SettableFuture<AppendEntriesResponse> future = SettableFuture.create();
    JsonObject message = new JsonObject()
        .putNumber("term", request.term())
        .putString("leader", request.leader())
        .putNumber("prevIndex", request.prevLogIndex())
        .putNumber("prevTerm", request.prevLogTerm())
        .putNumber("commit", request.commitIndex());
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setException(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            future.set(new AppendEntriesResponse(request.id(), result.result().body().getLong("term"), result.result().body().getBoolean("succeeded")));
          } else if (status.equals("error")) {
            future.set(new AppendEntriesResponse(request.id(), result.result().body().getString("message")));
          }
        }
      }
    });
    return future;
  }

  @Override
  public ListenableFuture<RequestVoteResponse> requestVote(final RequestVoteRequest request) {
    final SettableFuture<RequestVoteResponse> future = SettableFuture.create();
    JsonObject message = new JsonObject()
        .putNumber("term", request.term())
        .putString("candidate", request.candidate())
        .putNumber("lastIndex", request.lastLogIndex())
        .putNumber("lastTerm", request.lastLogTerm());
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setException(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            future.set(new RequestVoteResponse(request.id(), result.result().body().getLong("term"), result.result().body().getBoolean("voteGranted")));
          } else if (status.equals("error")) {
            future.set(new RequestVoteResponse(request.id(), result.result().body().getString("message")));
          }
        }
      }
    });
    return future;
  }

  @Override
  public ListenableFuture<SubmitCommandResponse> submitCommand(final SubmitCommandRequest request) {
    final SettableFuture<SubmitCommandResponse> future = SettableFuture.create();
    JsonObject message = new JsonObject()
        .putString("action", "requestVote")
        .putString("command", request.command())
        .putArray("args", new JsonArray(request.args()));
    vertx.eventBus().sendWithTimeout(address, message, 5000, new Handler<AsyncResult<Message<JsonObject>>>() {
      @Override
      public void handle(AsyncResult<Message<JsonObject>> result) {
        if (result.failed()) {
          future.setException(result.cause());
        } else {
          String status = result.result().body().getString("status");
          if (status.equals("ok")) {
            Object value = result.result().body().getValue("result");
            if (value instanceof JsonObject) {
              future.set(new SubmitCommandResponse(request.id(), ((JsonObject) value).toMap()));
            } else if (value instanceof JsonArray) {
              future.set(new SubmitCommandResponse(request.id(), ((JsonArray) value).toList()));
            } else {
              future.set(new SubmitCommandResponse(request.id(), value));
            }
          } else if (status.equals("error")) {
            future.set(new SubmitCommandResponse(request.id(), result.result().body().getString("message")));
          }
        }
      }
    });
    return future;
  }

  @Override
  public ListenableFuture<Void> connect() {
    if (vertx == null) {
      final SettableFuture<Void> future = SettableFuture.create();
      vertx = new DefaultVertx(port >= 0 ? port : 0, host, new Handler<AsyncResult<Vertx>>() {
        @Override
        public void handle(AsyncResult<Vertx> result) {
          if (result.failed()) {
            future.setException(result.cause());
          } else {
            future.set(null);
          }
        }
      });
      return future;
    } else {
      return Futures.immediateFuture(null);
    }
  }

  @Override
  public ListenableFuture<Void> close() {
    return Futures.immediateFuture(null);
  }

}
