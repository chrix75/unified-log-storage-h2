package com.github.chrix75.unifiedlog.storage.h2

import csperandio.unifiedlog.events.EventType

interface EventTypeBuilder {
    fun from(string: String): EventType

}
