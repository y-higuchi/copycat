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
package net.kuujo.copycat;

import net.kuujo.copycat.cluster.ClusterConfig;
import net.kuujo.copycat.endpoint.Endpoint;
import net.kuujo.copycat.endpoint.EndpointFactory;
import net.kuujo.copycat.endpoint.impl.DefaultEndpointFactory;
import net.kuujo.copycat.log.Log;
import net.kuujo.copycat.registry.Registry;

import com.google.common.base.Functions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Primary copycat API.<p>
 *
 * The <code>CopyCat</code> class provides a fluent API for
 * combining the {@link CopyCatContext} with an {@link Endpoint}.
 *
 * @author <a href="http://github.com/kuujo">Jordan Halterman</a>
 */
public class CopyCat {
  private final Endpoint endpoint;
  private final CopyCatContext context;

  public CopyCat(Endpoint endpoint, StateMachine stateMachine, ClusterConfig cluster) {
    this.endpoint = endpoint;
    this.context = new CopyCatContext(stateMachine, cluster);
  }

  public CopyCat(String uri, StateMachine stateMachine, ClusterConfig cluster) {
    this.context = new CopyCatContext(stateMachine, cluster);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  public CopyCat(String uri, StateMachine stateMachine, Log log, ClusterConfig cluster) {
    this.context = new CopyCatContext(stateMachine, log, cluster);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  public CopyCat(String uri, StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config) {
    this.context = new CopyCatContext(stateMachine, log, cluster, config);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  public CopyCat(String uri, StateMachine stateMachine, Log log, ClusterConfig cluster, CopyCatConfig config, Registry registry) {
    this.context = new CopyCatContext(stateMachine, log, cluster, config, registry);
    EndpointFactory factory = new DefaultEndpointFactory(context);
    this.endpoint = factory.createEndpoint(uri);
  }

  /**
   * Starts the replica.
   */
  @SuppressWarnings("unchecked")
  public ListenableFuture<Void> start() {
    return Futures.transform(Futures.allAsList(context.start(), endpoint.start()), Functions.<Void>constant(null));
  }

  /**
   * Stops the replica.
   *
   * @param callback A callback to be called once the replica has been stopped.
   */
  @SuppressWarnings("unchecked")
  public ListenableFuture<Void> stop() {
    return Futures.transform(Futures.allAsList(context.stop(), endpoint.stop()), Functions.<Void>constant(null));
  }

}
