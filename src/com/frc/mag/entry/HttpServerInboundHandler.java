package com.frc.mag.entry;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONNECTION;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.frc.mag.parse.Client;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpHeaders.Values;
import io.netty.handler.codec.http.HttpRequest;

public class HttpServerInboundHandler extends ChannelInboundHandlerAdapter {

    private static Log log = LogFactory.getLog(HttpServerInboundHandler.class);

    private HttpRequest request;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg)
            throws Exception {
    	String result = "";
        if (msg instanceof HttpRequest) {
            request = (HttpRequest) msg;

            String uri = request.getUri();
            System.out.println("Uri:" + uri);
            int idx = uri.lastIndexOf("?");
            String queryString = uri.substring(idx+1);
            String[] queryArr = queryString.split("&");
            String id1 = "", id2 = "";
            String arr[] = null;
            
            arr = queryArr[0].split("=");
            if (arr[0].indexOf("id1") >= 0) {
            	id1 = arr[1];
            } else {
            	id2 = arr[1];
            }
            
            arr = queryArr[1].split("=");
            if (arr[0].indexOf("id2") >= 0) {
            	id2 = arr[1];
            } else {
            	id1 = arr[1];
            }
            Client client = new Client();
            result = client.work(id1, id2);
        }
        if (msg instanceof HttpContent) {
            HttpContent content = (HttpContent) msg;
            ByteBuf buf = content.content();
            System.out.println(buf.toString(io.netty.util.CharsetUtil.UTF_8));
            buf.release();
        }
        FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                OK, Unpooled.wrappedBuffer(result.getBytes("UTF-8")));
        response.headers().set(CONTENT_TYPE, "text/plain");
        response.headers().set(CONTENT_LENGTH,
                response.content().readableBytes());
        if (HttpHeaders.isKeepAlive(request)) {
            response.headers().set(CONNECTION, Values.KEEP_ALIVE);
        }
        ctx.write(response);
        ctx.flush();
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) throws Exception {
        ctx.flush();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error(cause.getMessage());
        ctx.close();
    }

}