package com.github.chrix75.unifiedlog.storage.h2

import csperandio.unifiedlog.events.EventType
import csperandio.unifiedlog.events.SimpleEventType

class StringEventTypeBuilder : EventTypeBuilder {
    override fun from(s: String): EventType =
        SimpleEventType(s)

}