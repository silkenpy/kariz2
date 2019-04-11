package ir.rkr.kariz.netty

import com.github.benmanes.caffeine.cache.Cache
import com.github.benmanes.caffeine.cache.Caffeine
import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.epoll.EpollChannelOption
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.handler.timeout.ReadTimeoutHandler
import io.netty.handler.timeout.WriteTimeoutHandler
import io.netty.util.CharsetUtil
import ir.rkr.kariz.caffeine.CaffeineBuilder
import ir.rkr.kariz.kafka.KafkaConnector
import ir.rkr.kariz.redis.RedisConnector
import ir.rkr.kariz.util.KarizMetrics
import mu.KotlinLogging
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit


fun String.redisRequestParser(): List<String> = this.split("\r\n").filterIndexed { idx, _ -> idx % 2 == 0 }

data class Command(val cmd: String, val key: String, val value: String? = null, val ttl: Long? = null, val time: Long)

class RedisFeeder(val kafka: KafkaConnector, val caffeineCache: RedisConnector, val config: Config, val karizMetrics: KarizMetrics) : ChannelInboundHandlerAdapter() {

    private val logger = KotlinLogging.logger {}
    private val gson = GsonBuilder().disableHtmlEscaping().create()

    val tempCache: Cache<String, String>

    init {
        tempCache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.SECONDS)
                .build<String, String>()
    }

    private fun redisHandler(request: String): String {

        val parts = request.redisRequestParser()
        val forcedTtl = config.getInt("kariz.forcedTtl")
        karizMetrics.MarkNettyRequests(1)

        var command: String = ""

        val time = System.currentTimeMillis()
        try {
            when (parts[1].toLowerCase()) {


//                "select" -> return "+OK\r\n"

                "set" -> {

                    try {
                        if (parts.size == 6 && parts[4].toLowerCase() == "ex") {
                            command = gson.toJson(Command("set", parts[2], parts[3], parts[5].toLong(), time))

                        } else

                            if (parts.size == 4) {
                                if (forcedTtl == -1) {
                                    command = gson.toJson(Command("set", parts[2], parts[3], time = time))
                                } else {
                                    command = gson.toJson(Command("set", parts[2], parts[3], forcedTtl.toLong(), time))
                                }
                            }

                        return if (command.isNotEmpty() && kafka.put(parts[2], command))
                            "+OK\r\n"
                        else
                            "-Error in Set data to kafka\r\n"
                    } catch (e: Exception) {
                        return "-Error in Set\r\n"
                    }
                }


                "get" -> {
                    val value = caffeineCache.get(parts[2])
                    return if (value.isPresent)
                        "\$${value.get().length}\r\n${value.get()}\r\n"
                    else
                        "$-1\r\n"
                }


                "mset" -> {
                    return try {
                        for (i in 2..(parts.size - 1) step 2) {

                            command = gson.toJson(Command("set", parts[i], parts[i + 1], time = time))

                            val res = kafka.put(parts[i], command)
                            if (command.isEmpty() || !res) {
                                logger.warn { res }
                                return "-Error message1\r\n"
                            }
                        }
                        "+OK\r\n"

                    } catch (e: Exception) {
                        logger.error { e }
//                        logger.error { parts.toString() }
                        "-Error in Mset\r\n"
                    }
                }

                "mget" -> {
                    try {
                        var values = ""

                        for (i in 2..(parts.size - 1)) {
                            val value = caffeineCache.get(parts[i])
                            if (value.isPresent) {
                                values += "\$${value.get().length}\r\n${value.get()}\r\n"

                            } else values += "\$-1\r\n"
                        }

                        return "*${parts.size - 2}\r\n$values"
                    } catch (e: Exception) {
                        return "-Error in Mget\r\n"
                    }

                }

                "setex" -> {
                    command = gson.toJson(Command("set", parts[2], parts[4], parts[3].toLong(), time = time))
                    return if (command.isEmpty() || !kafka.put(parts[2], command))
                        "-Error in Setex\r\n"
                    else
                        "+OK\r\n"
                }

                "ping" -> return "+PONG\r\n"

                "del" -> {
                    var deletedNum = 0

                    return try {
                        for (i in 2..(parts.size - 1)) {
//                            println("i=$i")
                            command = gson.toJson(Command("del", parts[i], time = time))
                            if (kafka.put(parts[i], command)) deletedNum += 1
                        }
                        ":$deletedNum\r\n"
                    } catch (e: Exception) {
                        "-Error in Del\r\n"
                    }


                }

                "expire" -> {
                    command = gson.toJson(Command("expire", parts[2], ttl = parts[3].toLong(), time = time))
                    return if (parts.size == 4 && kafka.put(parts[2], command))
                        "+OK\r\n"
                    else
                        "-Error in Expire\r\n"
                }

                "command" -> return "+OK\r\n"

                "quit" -> return "+OK\r\n"

                "invalid" -> return "-Error message\r\n"

                else -> {
                    return "-Error Command not found\r\n"
                }

            }

        } catch (e: Exception) {

            logger.error { e }
            return "-Internal Error\r\n"
        }
    }


    private fun validator(id: String, tempRaw: String): String {

        val raw = tempCache.getIfPresent(id).orEmpty() + tempRaw

        if (raw.isEmpty())
            return ""

        val command = raw.split("\r\n")

        if (command[0].startsWith("*")) {
            val partsNum = command[0].split("*")[1].toInt()

            try {
                for (i in 1..partsNum * 2 step 2) {

                    if (command[i].split("$")[1].toInt() != command[i + 1].length) {
                        tempCache.put(id, raw)
                        return ""
                    }
                }

                if ((partsNum * 2 + 1) != (command.size - 1)) {
                    logger.error { command }
                    return "*1\r\n$7\r\ninvalid\r\n"
                }

                tempCache.invalidate(id)
                return raw

            } catch (e: Exception) {
                tempCache.put(id, raw)
                return ""
            }


        } else {
            logger.error { "Invalid Command or data $raw" }
            return "*1\r\n$7\r\ninvalid\r\n"
        }

    }

    @Throws(Exception::class)
    override fun channelRead(ctx: ChannelHandlerContext, msg: Any) {

        val inBuffer = (msg as ByteBuf)

        val command = validator("${ctx.channel().remoteAddress()}${ctx.channel().id()}", inBuffer.toString(CharsetUtil.US_ASCII))
        inBuffer.release()
//        logger.trace { ctx.channel().remoteAddress().toString() + "  " + ctx.channel().id() + "  " + command }

        if (command.isNotEmpty()) {
            ctx.writeAndFlush(Unpooled.copiedBuffer(redisHandler(command), CharsetUtil.US_ASCII))
        }
//        ctx.close()
    }

    @Throws(Exception::class)
    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        cause.printStackTrace()
        ctx.close()
    }
}

class NettyServer(val kafka: KafkaConnector, val caffeineCache: RedisConnector, config: Config, val karizMetrics: KarizMetrics) {


    init {

        val parent = EpollEventLoopGroup(40).apply { setIoRatio(70) }
        val child = EpollEventLoopGroup(40).apply { setIoRatio(70) }

        val serverBootstrap = ServerBootstrap().group(parent, child)
        serverBootstrap.channel(EpollServerSocketChannel::class.java)
        serverBootstrap.localAddress(InetSocketAddress(config.getString("kariz.ip"), config.getInt("kariz.port")))

        serverBootstrap.option(EpollChannelOption.TCP_NODELAY, true)
        serverBootstrap.option(EpollChannelOption.AUTO_READ, true)
        serverBootstrap.option(EpollChannelOption.SO_REUSEADDR, true)
        serverBootstrap.option(EpollChannelOption.CONNECT_TIMEOUT_MILLIS, 50000)
        serverBootstrap.option(EpollChannelOption.SO_REUSEPORT, true)
        serverBootstrap.option(EpollChannelOption.SO_RCVBUF, 1024 * 1024 * 100)

        serverBootstrap.childOption(EpollChannelOption.SO_KEEPALIVE, true)
        serverBootstrap.childOption(EpollChannelOption.SO_REUSEADDR, true)
        serverBootstrap.childOption(EpollChannelOption.TCP_NODELAY, true)
        serverBootstrap.childOption(EpollChannelOption.SO_RCVBUF, 1024 * 1024 * 100)

        serverBootstrap.childHandler(object : ChannelInitializer<EpollSocketChannel>() {
            @Throws(Exception::class)
            override fun initChannel(socketChannel: EpollSocketChannel) {

                val pipeline = socketChannel.pipeline()

                pipeline.addLast("readTimeoutHandler", ReadTimeoutHandler(3000))
                pipeline.addLast("writeTimeoutHandler", WriteTimeoutHandler(3000))

                pipeline.addLast(RedisFeeder(kafka, caffeineCache, config, karizMetrics))
            }
        })

        val f = serverBootstrap.bind().sync()
//        f.channel().closeFuture().sync()


    }
}