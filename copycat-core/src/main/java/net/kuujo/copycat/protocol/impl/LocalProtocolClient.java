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
package net.kuujo.copycat.protocol.impl;

import net.kuujo.copycat.CopyCatContext;
import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Local protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class LocalProtocolClient implements ProtocolClient {
  private final String address;
  private final CopyCatContext context;

  public LocalProtocolClient(String address, CopyCatContext context) {
    this.address = address;
    this.context = context;
  }

  @Override
  public ListenableFuture<AppendEntriesResponse> appendEntries(AppendEntriesRequest request) {
    LocalProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      return server.appendEntries(request);
    }
    return Futures.immediateFailedFuture(new ProtocolException("Invalid server address"));
  }

  @Override
  public ListenableFuture<RequestVoteResponse> requestVote(RequestVoteRequest request) {
    LocalProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      server.requestVote(request);
    }
    return Futures.immediateFailedFuture(new ProtocolException("Invalid server address"));
  }

  @Override
  public ListenableFuture<SubmitCommandResponse> submitCommand(SubmitCommandRequest request) {
    LocalProtocolServer server = context.registry().lookup(address);
    if (server != null) {
      return server.submitCommand(request);
    }
    return Futures.immediateFailedFuture(new ProtocolException("Invalid server address"));
  }

  @Override
  public ListenableFuture<Void> connect() {
    return Futures.immediateFuture(null);
  }

  @Override
  public ListenableFuture<Void> close() {
    return Futures.immediateFuture(null);
  }

}
