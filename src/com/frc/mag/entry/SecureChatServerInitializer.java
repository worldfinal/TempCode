package com.frc.mag.entry;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.DelimiterBasedFrameDecoder;
import io.netty.handler.codec.Delimiters;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;

/**
 * Creates a newly configured {@link io.netty.channel.ChannelPipeline} for a new channel.
 */
public class SecureChatServerInitializer extends ChannelInitializer<SocketChannel> {

    @Override
    public void initChannel(SocketChannel ch) throws Exception {
        ChannelPipeline pipeline = ch.pipeline();

        // Add SSL handler first to encrypt and decrypt everything.
        // In this example, we use a bogus certificate in the server side
        // and accept any invalid certificates in the client side.
        // You will need something more complicated to identify both
        // and server in the real world.
        //
        // Read SecureChatSslContextFactory
        // if you need client certificate authentication.

        // On top of the SSL handler, add the text line codec.
        pipeline.addLast("decoder", new HttpResponseEncoder());
        pipeline.addLast("encoder", new HttpRequestDecoder());
        // and then business logic.
        pipeline.addLast("handler", new HttpServerInboundHandler());
    }
}
