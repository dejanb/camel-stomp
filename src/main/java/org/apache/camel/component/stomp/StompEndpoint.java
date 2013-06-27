/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.camel.component.stomp;

import org.apache.camel.Consumer;
import org.apache.camel.Message;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.impl.DefaultEndpoint;
import org.fusesource.hawtdispatch.Dispatcher;
import org.fusesource.hawtdispatch.Task;
import org.fusesource.stomp.client.Callback;
import org.fusesource.stomp.client.CallbackConnection;
import org.fusesource.stomp.client.Promise;

import org.fusesource.stomp.codec.StompFrame;

import static org.fusesource.hawtbuf.UTF8Buffer.utf8;
import static org.fusesource.stomp.client.Constants.*;

import java.util.concurrent.TimeUnit;

public class StompEndpoint extends DefaultEndpoint {

    private CallbackConnection connection;
    private StompConfiguration configuration;
    private String destination;

    public StompEndpoint(String uri, StompComponent component, StompConfiguration configuration, String destination) {
        super(uri, component);
        this.configuration = configuration;
        this.destination = destination;
    }

    public Producer createProducer() throws Exception {
        return new StompProducer(this);
    }

    public Consumer createConsumer(Processor processor) throws Exception {
        return null;
    }

    public boolean isSingleton() {
        return true;
    }

    @Override
    protected void doStart() throws Exception {
        final Promise<CallbackConnection> promise = new Promise<CallbackConnection>();

        configuration.getStomp().connectCallback(promise);

        connection = promise.await();
    }

    protected void send(Message message) {
       final StompFrame frame = new StompFrame(SEND);
       frame.addHeader(DESTINATION, StompFrame.encodeHeader(destination));
       frame.content(utf8(message.getBody().toString()));
       connection.getDispatchQueue().execute(new Task() {
           @Override
           public void run() {
               connection.send(frame, new Callback<Void>() {
                   @Override
                   public void onSuccess(Void value) {
                       //System.out.println("sent");
                   }

                   @Override
                   public void onFailure(Throwable value) {
                       //System.out.println("failure " + value);
                   }
               });
           }
       });
    }

    @Override
    protected String createEndpointUri() {
        return super.createEndpointUri();
    }
}
