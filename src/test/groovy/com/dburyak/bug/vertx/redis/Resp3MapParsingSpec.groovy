package com.dburyak.bug.vertx.redis

import io.vertx.core.Completable
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisConnection
import io.vertx.redis.client.Request
import org.testcontainers.containers.GenericContainer
import org.testcontainers.spock.Testcontainers
import spock.lang.Shared
import spock.lang.Specification
import spock.lang.Subject

import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException

import static io.vertx.redis.client.Command.HGETALL
import static io.vertx.redis.client.Command.HSET
import static java.nio.charset.StandardCharsets.UTF_8

/**
 * We will use HGETALL command to demonstrate the bug. But it's not specific to HGETALL, any RESP3 command that returns
 * a "map" type will have the same issue.
 */
@Testcontainers
class Resp3MapParsingSpec extends Specification {
    public static final String REDIS_VERSION = '8.0.3'
    public static final int REDIS_PORT = 6379

    @Subject
    RedisConnection redis

    @Shared
    Redis redisClient

    @Shared
    Vertx vertx

    // Verticle/context/EL-thread where all the operations that interact with vertx entities are executed.
    // We won't be calling vertx objects from junit thread in order to avoid any potential concurrency issues.
    @Shared
    TestVerticle testVerticle = new TestVerticle()

    @Shared
    GenericContainer redisContainer = new GenericContainer("redis:$REDIS_VERSION").withExposedPorts(REDIS_PORT)

    def setupSpec() {
        vertx = Vertx.vertx()
        vertx.deployVerticle(testVerticle).await() // we block junit thread here
        // now we can run vertx operations on the verticle's context
        redisClient = syncOnVertx {
            Redis.createClient(vertx, "redis://$redisContainer.host:${redisContainer.getMappedPort(REDIS_PORT)}")
        }
    }

    def cleanupSpec() {
        onVertx {
            (redisClient?.close() ?: Future.<Void> succeededFuture())
                    .andThen({ vertx.close() } as Handler)
        }
    }

    def setup() {
        redis = onVertx { redisClient.connect() }
    }

    def cleanup() {
        onVertx { redis.close() }
    }


    def 'hgetall - works fine when HASH fields are utf-8 strings'() {
        given: 'redis HASH with fields being valid utf-8 strings'
        def field1 = 'test-field-1'
        def field2 = 'test-field-2'
        def value1 = 'test-value-1'
        def value2 = 'test-value-2'
        def key = 'test-hash-text'
        onVertx {
            redis.send Request.cmd(HSET).arg(key)
                    .arg(field1).arg(value1)
                    .arg(field2).arg(value2)
        }

        when: 'we retrieve the hash with HGETALL'
        def hgetAllRes = onVertx {
            redis.send Request.cmd(HGETALL).arg(key)
        }

        then: 'fields are decoded correctly'
        hgetAllRes.size() == 4 // 2 fields + 2 values
        hgetAllRes.getKeys() == [field1, field2] as Set

        and: 'values are also decoded correctly'
        // note that values ore of BulkType type and they can contain binary data, and we decode them as utf-8 strings
        // before comparing with expected values (strings)
        hgetAllRes[field1].toString() == value1
        hgetAllRes[field2].toString() == value2
    }


    def 'hgetall - does not work when HASH fields are binary data'() {
        given: 'redis HASH with fields being binary data'
        def field1 = new byte[] {0x01, 0x02, 0x03} // positive bytes will actually work fine since they are valid utf-8
        def field2 = new byte[] {-0x01, -0x02, -0x03} // negative bytes (unsigned bytes > 127) will get scrambled though
        // values are handled correctly since they are not assumed to be utf-8 strings, and BulkType is used
        def value1 = new byte[] {0x04, 0x05, 0x06}
        def value2 = new byte[] {-0x04, -0x05, -0x06}
        def key = 'test-hash-binary'
        onVertx {
            redis.send Request.cmd(HSET).arg(key)
                    .arg(field1).arg(value1)
                    .arg(field2).arg(value2)
        }

        when: 'we retrieve the hash with HGETALL'
        def hgetAllRes = onVertx {
            redis.send Request.cmd(HGETALL).arg(key)
        }

        then: 'fields can not be decoded in such way that the original data is preserved'
        // First issue is that there's no API to get them as binary directly, only as utf-8 strings.
        // Secondly, even if we try to restore the binary data from utf-8 strings somehow, it not possible to do so.
        hgetAllRes.size() == 4 // 2 fields + 2 values
        def retrievedField1 = hgetAllRes.getKeys().find { it == new String(field1, UTF_8) }
        def retrievedField2 = hgetAllRes.getKeys().find { it != new String(field1, UTF_8) }
        def retrievedField1AsBytes = retrievedField1.getBytes(UTF_8)
        def retrievedField2AsBytes = retrievedField2.getBytes(UTF_8)
        println "key1: str=$retrievedField1, bytes=$retrievedField1AsBytes"
        println "key2: str=$retrievedField2, bytes=$retrievedField2AsBytes"
        retrievedField1AsBytes == field1 // this works fine, since positive bytes are valid utf-8

        // this does not work, since negative bytes are not valid utf-8 and get scrambled
        retrievedField2AsBytes != field2
        def retrievedField2AsCodepoints = retrievedField2.codePoints().boxed().toList()
        println "key2: codepoints=${retrievedField2AsCodepoints.join(', ')}"
        retrievedField2AsCodepoints == [65533, 65533, 65533]
    }


    // ------------------------- HELPER METHODS --------------------------

    def <T> T onVertx(Closure<Future<T>> action) {
        def result = new CompletableFuture<T>()
        testVerticle.context.runOnContext {
            try {
                action.call().onComplete({ res, err ->
                    if (err) {
                        result.completeExceptionally(err)
                    } else {
                        result.complete(res)
                    }
                } as Completable<T>)
            } catch (Throwable e) {
                result.completeExceptionally(e)
            }
        }
        try {
            // blocks junit thread until vertx async operation is completed, this bridges async vertx => sync junit
            result.get()
        } catch (ExecutionException execErr) {
            throw execErr.cause ?: execErr
        }
    }

    def <T> T syncOnVertx(Closure<T> action) {
        def result = new CompletableFuture<T>()
        testVerticle.context.runOnContext {
            try {
                result.complete(action.call())
            } catch (Throwable e) {
                result.completeExceptionally(e)
            }
        }
        try {
            // blocks junit thread until vertx async operation is completed, this bridges async vertx => sync junit
            result.get()
        } catch (ExecutionException execErr) {
            throw execErr.cause ?: execErr
        }
    }
}
