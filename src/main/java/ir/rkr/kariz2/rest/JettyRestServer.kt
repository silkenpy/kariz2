package ir.rkr.kariz2.rest

import com.google.common.util.concurrent.RateLimiter
import com.google.gson.GsonBuilder
import com.typesafe.config.Config
import ir.rkr.kariz2.redis.RedisConnector
import ir.rkr.kariz2.util.KarizMetrics
import ir.rkr.kariz2.util.fromJson
import org.eclipse.jetty.http.HttpStatus
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.thread.QueuedThreadPool
import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

/**
 * [JettyRestServer] is a rest-based service to handle requests of redis cluster with an additional
 * in-memory cache layer based on ignite to increase performance and decrease number of requests of
 * redis cluster.
 */


/**
 * [Results] is a data model for responses.
 */
data class Results(var results: HashMap<String, String> = HashMap<String, String>())

class JettyRestServer(val Redis: RedisConnector, val config: Config, val karizMetrics: KarizMetrics) : HttpServlet() {

    private val gson = GsonBuilder().disableHtmlEscaping().create()
    private val urlRateLimiter = RateLimiter.create(config.getDouble("check.url.rateLimit"))
    private val tagRateLimiter = RateLimiter.create(config.getDouble("check.tag.rateLimit"))
    private val usrRateLimiter = RateLimiter.create(config.getDouble("check.usr.rateLimit"))

    /**
     * This function [checkUrl] is used to ask value of a key from ignite server or redis server and update
     * ignite cluster.
     */

    fun checkUrl(key: String): String {

        karizMetrics.MarkCheckUrl(1)

        if (!urlRateLimiter.tryAcquire()) return ""

        val value = Redis.get(key)

        return if (value.isPresent) {
            karizMetrics.MarkUrlInRedis(1)
            value.get()

        } else {

            karizMetrics.MarkUrlNotInRedis(1)
            ""
        }
    }


    /**
     * This function [checkTag] is used to ask value of a key from ignite server or redis server and update
     * ignite cluster.
     */

    fun checkTag(key: String): String {

        karizMetrics.MarkCheckTag(1)

        if (!tagRateLimiter.tryAcquire()) return ""

        val value = Redis.get(key)

        return if (value.isPresent) {
            karizMetrics.MarkTagInRedis(1)
            value.get()

        } else {

            karizMetrics.MarkTagNotInRedis(1)
            ""
        }
    }

    /**
     * This function [checkUsr] is used to ask value of a key from ignite server or redis server and update
     * ignite cluster.
     */
    private fun checkUsr(key: String): String {
        karizMetrics.MarkCheckUsr(1)

        if (!usrRateLimiter.tryAcquire()) return "0"

        val value = Redis.get(key)

        return if (value.isPresent) {
            karizMetrics.MarkUsrInRedis(1)
            value.get()

        } else {

            karizMetrics.MarkUsrNotInRedis(1)
            "0"
        }
    }

    /**
     * Start a jetty server.
     */
    init {
        val threadPool = QueuedThreadPool( config.getInt("jetty.threadNumMin"),  config.getInt("jetty.threadNumMax"))
        val server = Server(threadPool)
        val http = ServerConnector(server).apply {
            host = config.getString("jetty.ip")
            port = config.getInt("jetty.port")
        }

        server.addConnector(http)

        val handler = ServletContextHandler(server, "/")

        /**
         * It can handle multi-get requests for Urls in json format.
         */
        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
                val msg = Results()

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())

                karizMetrics.MarkUrlBatches(1)
                Redis.mget(parsedJson.toList()).forEach { k, v ->
                    if (!k.isNullOrBlank())
                        if (v.isPresent){
                            karizMetrics.MarkUrlInRedis(1)
                            msg.results[k] = config.getString("router")+v.get()
                        }
                        else{
                            karizMetrics.MarkUrlNotInRedis(1)
                           msg.results[k]= ""
                        }
                }
//                for (key in parsedJson) {
//
//                    if (key.isNotEmpty()) msg.results[key] = checkUrl(key)
//                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(msg.results))
                }
            }
        }), "/cache/url")


        /**
         * It can handle multi-get requests for Tags in json format.
         */
        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
                val msg = Results()

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())

                karizMetrics.MarkTagBatches(1)
                for (key in parsedJson) {

                    if (key.isNotEmpty()) msg.results[key] = checkTag(key)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(msg.results))
                }
            }
        }), "/cache/tag")

        /**
         * It can handle multi-get requests for Usrs in json format.
         */
        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doPost(req: HttpServletRequest, resp: HttpServletResponse) {
                val msg = Results()

                val parsedJson = gson.fromJson<Array<String>>(req.reader.readText())
                karizMetrics.MarkUsrBatches(1)
                for (key in parsedJson) {
                    if (key.isNotEmpty()) msg.results[key] = checkUsr(key)
                }

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(msg.results))
                }
            }
        }), "/cache/usr")

        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {

                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "application/json; charset=utf-8")
                    //addHeader("Connection", "close")
                    writer.write(gson.toJson(karizMetrics.getInfo()))
                }
            }
        }), "/metrics")

        handler.addServlet(ServletHolder(object : HttpServlet() {
            override fun doGet(req: HttpServletRequest, resp: HttpServletResponse) {
                resp.apply {
                    status = HttpStatus.OK_200
                    addHeader("Content-Type", "text/plain; charset=utf-8")
//                    addHeader("Connection", "close")
                    writer.write("server  is running :D")
                }
            }
        }), "/version")

        server.start()

    }
}