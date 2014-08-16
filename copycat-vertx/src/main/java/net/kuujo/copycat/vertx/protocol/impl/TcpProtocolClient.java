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

import java.util.HashMap;
import java.util.Map;

import net.kuujo.copycat.log.Entry;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;
import net.kuujo.copycat.serializer.Serializer;
import net.kuujo.copycat.serializer.SerializerFactory;

import org.vertx.java.core.AsyncResult;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.impl.DefaultVertx;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.parsetools.RecordParser;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Vert.x TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpProtocolClient implements ProtocolClient {
  private static final Serializer serializer = SerializerFactory.getSerializer();
  private Vertx vertx;
  private final String host;
  private final int port;
  private boolean trustAll;
  private final TcpProtocol protocol;
  private NetClient client;
  private NetSocket socket;
  private final Map<Object, ResponseHolder> responses = new HashMap<>();

  /**
   * Holder for response handlers.
   */
  @SuppressWarnings("rawtypes")
  private static class ResponseHolder {
    private final SettableFuture future;
    private final ResponseType type;
    private final long timer;
    private ResponseHolder(long timerId, ResponseType type, SettableFuture future) {
      this.timer = timerId;
      this.type = type;
      this.future = future;
    }
  }

  /**
   * Indicates response types.
   */
  private static enum ResponseType {
    APPEND,
    VOTE,
    SUBMIT;
  }

  public TcpProtocolClient(String host, int port, TcpProtocol protocol) {
    this.host = host;
    this.port = port;
    this.protocol = protocol;
  }

  /**
   * Sets whether to trust all server certs.
   *
   * @param trustAll Whether to trust all server certs.
   */
  public void setTrustAll(boolean trustAll) {
    this.trustAll = trustAll;
  }

  /**
   * Returns whether to trust all server certs.
   *
   * @return Whether to trust all server certs.
   */
  public boolean isTrustAll() {
    return trustAll;
  }

  /**
   * Sets whether to trust all server certs, returning the protocol for method chaining.
   *
   * @param trustAll Whether to trust all server certs.
   * @return The TCP protocol.
   */
  public TcpProtocolClient withTrustAll(boolean trustAll) {
    this.trustAll = trustAll;
    return this;
  }

  @Override
  public ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
    if (socket != null) {
      JsonArray jsonEntries = new JsonArray();
      for (Entry entry : request.entries()) {
        jsonEntries.addString(new String(serializer.writeValue(entry)));
      }
      socket.write(new JsonObject().putString("type", "append")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("leader", request.leader())
          .putNumber("prevIndex", request.prevLogIndex())
          .putNumber("prevTerm", request.prevLogTerm())
          .putArray("entries", jsonEntries)
          .putNumber("commit", request.commitIndex()).encode() + '\00');
      return storeFuture(request.id(), ResponseType.APPEND, SettableFuture.<AppendEntriesResponse>create());
    } else {
      return Futures.immediateFailedFuture(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public ListenableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "vote")
          .putValue("id", request.id())
          .putNumber("term", request.term())
          .putString("candidate", request.candidate())
          .putNumber("lastIndex", request.lastLogIndex())
          .putNumber("lastTerm", request.lastLogTerm())
          .encode() + '\00');
      return storeFuture(request.id(), ResponseType.VOTE, SettableFuture.<RequestVoteResponse>create());
    } else {
      return Futures.immediateFailedFuture(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public ListenableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request) {
    if (socket != null) {
      socket.write(new JsonObject().putString("type", "submit")
          .putValue("id", request.id())
          .putString("command", request.command())
          .putArray("args", new JsonArray(request.args()))
          .encode() + '\00');
      return storeFuture(request.id(), ResponseType.SUBMIT, SettableFuture.<SubmitCommandResponse>create());
    } else {
      return Futures.immediateFailedFuture(new ProtocolException("Client not connected"));
    }
  }

  /**
   * Handles an identifiable response.
   */
  @SuppressWarnings("unchecked")
  private void handleResponse(Object id, JsonObject response) {
    ResponseHolder holder = responses.remove(id);
    if (holder != null) {
      vertx.cancelTimer(holder.timer);
      switch (holder.type) {
        case APPEND:
          handleAppendResponse(response, (SettableFuture<AppendEntriesResponse>) holder.future);
          break;
        case VOTE:
          handleVoteResponse(response, (SettableFuture<RequestVoteResponse>) holder.future);
          break;
        case SUBMIT:
          handleSubmitResponse(response, (SettableFuture<SubmitCommandResponse>) holder.future);
          break;
      }
    }
  }

  /**
   * Handles an append entries response.
   */
  private void handleAppendResponse(JsonObject response, SettableFuture<AppendEntriesResponse> future) {
    String status = response.getString("status");
    if (status == null) {
      future.setException(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      future.set(new AppendEntriesResponse(response.getValue("id"), response.getLong("term"), response.getBoolean("succeeded")));
    } else if (status.equals("error")) {
      future.setException(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Handles a vote response.
   */
  private void handleVoteResponse(JsonObject response, SettableFuture<RequestVoteResponse> future) {
    String status = response.getString("status");
    if (status == null) {
      future.setException(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      future.set(new RequestVoteResponse(response.getValue("id"), response.getLong("term"), response.getBoolean("voteGranted")));
    } else if (status.equals("error")) {
      future.setException(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Handles a submit response.
   */
  private void handleSubmitResponse(JsonObject response, SettableFuture<SubmitCommandResponse> future) {
    String status = response.getString("status");
    if (status == null) {
      future.setException(new ProtocolException("Invalid response"));
    } else if (status.equals("ok")) {
      future.set(new SubmitCommandResponse(response.getValue("id"), response.getObject("result").toMap()));
    } else if (status.equals("error")) {
      future.setException(new ProtocolException(response.getString("message")));
    }
  }

  /**
   * Stores a response callback by ID.
   */
  private <T extends Response> ListenableFuture<T> storeFuture(final Object id, ResponseType responseType, SettableFuture<T> future) {
    long timerId = vertx.setTimer(30000, new Handler<Long>() {
      @Override
      public void handle(Long timerID) {
        ResponseHolder holder = responses.remove(id);
        if (holder != null) {
          holder.future.setException(new ProtocolException("Request timed out"));
        }
      }
    });
    ResponseHolder holder = new ResponseHolder(timerId, responseType, future);
    responses.put(id, holder);
    return future;
  }

  @Override
  public ListenableFuture<Void> connect() {
    final SettableFuture<Void> future = SettableFuture.create();
    if (vertx == null) {
      vertx = new DefaultVertx();
    }

    if (client == null) {
      client = vertx.createNetClient();
      client.setTCPKeepAlive(true);
      client.setTCPNoDelay(true);
      client.setSendBufferSize(protocol.getSendBufferSize());
      client.setReceiveBufferSize(protocol.getReceiveBufferSize());
      client.setSSL(protocol.isSsl());
      client.setKeyStorePath(protocol.getKeyStorePath());
      client.setKeyStorePassword(protocol.getKeyStorePassword());
      client.setTrustStorePath(protocol.getTrustStorePath());
      client.setTrustStorePassword(protocol.getTrustStorePassword());
      client.setTrustAll(trustAll);
      client.setUsePooledBuffers(true);
      client.connect(port, host, new Handler<AsyncResult<NetSocket>>() {
        @Override
        public void handle(AsyncResult<NetSocket> result) {
          if (result.failed()) {
            future.setException(result.cause());
          } else {
            socket = result.result();
            socket.dataHandler(RecordParser.newDelimited(new byte[]{'\00'}, new Handler<Buffer>() {
              @Override
              public void handle(Buffer buffer) {
                JsonObject response = new JsonObject(buffer.toString());
                Object id = response.getValue("id");
                handleResponse(id, response);
              }
            }));
            future.set(null);
          }
        }
      });
    } else {
      future.set(null);
    }
    return future;
  }

  @Override
  public ListenableFuture<Void> close() {
    final SettableFuture<Void> future = SettableFuture.create();
    if (client != null && socket != null) {
      socket.closeHandler(new Handler<Void>() {
        @Override
        public void handle(Void event) {
          socket = null;
          client.close();
          client = null;
          future.set(null);
        }
      }).close();
    } else if (client != null) {
      client.close();
      client = null;
      future.set(null);
    } else {
      future.set(null);
    }
    return future;
  }

}
