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
package net.kuujo.copycat.netty.protocol.impl;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.netty.handler.codec.serialization.ObjectEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.SSLException;

import net.kuujo.copycat.protocol.AppendEntriesRequest;
import net.kuujo.copycat.protocol.AppendEntriesResponse;
import net.kuujo.copycat.protocol.ProtocolClient;
import net.kuujo.copycat.protocol.ProtocolException;
import net.kuujo.copycat.protocol.RequestVoteRequest;
import net.kuujo.copycat.protocol.RequestVoteResponse;
import net.kuujo.copycat.protocol.Response;
import net.kuujo.copycat.protocol.SubmitCommandRequest;
import net.kuujo.copycat.protocol.SubmitCommandResponse;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

/**
 * Netty TCP protocol client.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class TcpProtocolClient implements ProtocolClient {
  private final TcpProtocol protocol;
  private Channel channel;
  private final Map<Object, SettableFuture<? extends Response>> responseFutures = new HashMap<>();

  public TcpProtocolClient(TcpProtocol protocol) {
    this.protocol = protocol;
  }

  @Override
  public ListenableFuture<AppendEntriesResponse> appendEntries(final AppendEntriesRequest request) {
    if (channel != null) {
      final SettableFuture<AppendEntriesResponse> future = SettableFuture.create();
      channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
          if (channelFuture.isSuccess()) {
            responseFutures.put(request.id(), future);
          } else {
            future.setException(new ProtocolException(channelFuture.cause()));
          }
        }
      });
      return future;
    } else {
      return Futures.immediateFailedFuture(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public ListenableFuture<RequestVoteResponse> requestVote(final RequestVoteRequest request) {
    if (channel != null) {
      final SettableFuture<RequestVoteResponse> future = SettableFuture.create();
      channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
          if (channelFuture.isSuccess()) {
            responseFutures.put(request.id(), future);
          } else {
            future.setException(new ProtocolException(channelFuture.cause()));
          }
        }
      });
      return future;
    } else {
      return Futures.immediateFailedFuture(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public ListenableFuture<SubmitCommandResponse> submitCommand(final SubmitCommandRequest request) {
    if (channel != null) {
      final SettableFuture<SubmitCommandResponse> future = SettableFuture.create();
      channel.writeAndFlush(request).addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
          if (channelFuture.isSuccess()) {
            responseFutures.put(request.id(), future);
          } else {
            future.setException(new ProtocolException(channelFuture.cause()));
          }
        }
      });
      return future;
    } else {
      return Futures.immediateFailedFuture(new ProtocolException("Client not connected"));
    }
  }

  @Override
  public ListenableFuture<Void> connect() {
    if (channel != null) {
      return Futures.immediateFuture(null);
    }

    final SslContext sslContext;
    if (protocol.isSsl()) {
      try {
        sslContext = SslContext.newClientContext(InsecureTrustManagerFactory.INSTANCE);
      } catch (SSLException e) {
        return Futures.immediateFailedFuture(e);
      }
    } else {
      sslContext = null;
    }

    final EventLoopGroup group = new NioEventLoopGroup(protocol.getThreads());
    Bootstrap bootstrap = new Bootstrap();
    bootstrap.group(group)
      .channel(NioSocketChannel.class)
      .handler(new ChannelInitializer<SocketChannel>() {
        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
          ChannelPipeline pipeline = channel.pipeline();
          if (sslContext != null) {
            pipeline.addLast(sslContext.newHandler(channel.alloc(), protocol.getHost(), protocol.getPort()));
          }
          pipeline.addLast(
              new ObjectEncoder(),
              new ObjectDecoder(ClassResolvers.softCachingConcurrentResolver(getClass().getClassLoader())),
              new TcpProtocolClientHandler(TcpProtocolClient.this)
          );
        }
      });

    if (protocol.getSendBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_SNDBUF, protocol.getSendBufferSize());
    }

    if (protocol.getReceiveBufferSize() > -1) {
      bootstrap.option(ChannelOption.SO_RCVBUF, protocol.getReceiveBufferSize());
    }

    if (protocol.getTrafficClass() > -1) {
      bootstrap.option(ChannelOption.IP_TOS, protocol.getTrafficClass());
    }

    bootstrap.option(ChannelOption.TCP_NODELAY, protocol.isNoDelay());
    bootstrap.option(ChannelOption.SO_LINGER, protocol.getSoLinger());
    bootstrap.option(ChannelOption.SO_KEEPALIVE, protocol.isKeepAlive());
    bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, protocol.getConnectTimeout());

    final SettableFuture<Void> future = SettableFuture.create();
    bootstrap.connect(protocol.getHost(), protocol.getPort()).addListener(new ChannelFutureListener() {
      @Override
      public void operationComplete(ChannelFuture channelFuture) throws Exception {
        if (channelFuture.isSuccess()) {
          channel = channelFuture.channel();
          future.set(null);
        } else if (channelFuture.cause() != null) {
          future.setException(channelFuture.cause());
        }
      }
    });
    return future;
  }

  @Override
  public ListenableFuture<Void> close() {
    if (channel != null) {
      final SettableFuture<Void> future = SettableFuture.create();
      channel.close().addListener(new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture channelFuture) throws Exception {
          channel = null;
          if (channelFuture.isSuccess()) {
            future.set(null);
          } else if (channelFuture.cause() != null) {
            future.setException(channelFuture.cause());
          }
        }
      });
      return future;
    } else {
      return Futures.immediateFuture(null);
    }
  }

  /**
   * Client response handler.
   */
  private static class TcpProtocolClientHandler extends ChannelInboundHandlerAdapter {
    private final TcpProtocolClient client;

    private TcpProtocolClientHandler(TcpProtocolClient client) {
      this.client = client;
    }

    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void channelRead(final ChannelHandlerContext context, Object message) {
      Response response = (Response) message;
      SettableFuture responseFuture = client.responseFutures.remove(response.id());
      if (responseFuture != null) {
        responseFuture.set(response);
      }
    }
  }

}
