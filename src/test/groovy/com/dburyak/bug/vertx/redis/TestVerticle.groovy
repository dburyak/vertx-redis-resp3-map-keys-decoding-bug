package com.dburyak.bug.vertx.redis

import io.vertx.core.AbstractVerticle
import io.vertx.core.Context
import io.vertx.core.Vertx

/**
 * Makes verticle context available for tests.
 */
class TestVerticle extends AbstractVerticle {
    Context context

    @Override
    void init(Vertx vertx, Context context) {
        super.init(vertx, context)
        this.context = context
    }
}
