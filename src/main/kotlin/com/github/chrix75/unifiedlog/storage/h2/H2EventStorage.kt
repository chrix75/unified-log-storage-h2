package com.github.chrix75.unifiedlog.storage.h2

import csperandio.unifiedlog.events.Event
import csperandio.unifiedlog.storage.EventStorage
import java.sql.Connection
import java.sql.Date
import javax.sql.DataSource

class H2EventStorage(private val ds: DataSource, typeBuilder: EventTypeBuilder, pageSize: Int = 10) : EventStorage {

    private val page = StoragePage(pageSize, ds, typeBuilder)

    override fun save(e: Event) {
        ds.connection.use { c ->
            saveEvent(c, e)
        }
    }

    private fun saveEvent(c: Connection, e: Event) {
        val st = c.prepareStatement(
            "INSERT INTO LOG_EVENT (EVENT_ID, EVENT_TYPE, EVENT_DATA, EVENT_TIMESTAMP) " +
                    "VALUES(?, ?, ?, ?)"
        )
        st.setString(1, e.id.toString())
        st.setString(2, e.type.name)
        st.setString(3, String(e.data))
        st.setDate(4, Date(e.timestamp.time))

        st.use { it.execute() }
    }

    override operator fun get(i: Int): Event = page[i]

    fun createTablesIfNotExist() {
        ds.connection.use { c ->
            val st = c.prepareStatement(
                "CREATE TABLE IF NOT EXISTS LOG_EVENT (" +
                        "EVENT_SEQ INT AUTO_INCREMENT(1, 1)," +
                        "EVENT_ID VARCHAR2(50) PRIMARY KEY," +
                        "EVENT_TYPE VARCHAR2(100) NOT NULL," +
                        "EVENT_DATA CLOB NOT NULL," +
                        "EVENT_TIMESTAMP TIMESTAMP)"
            )

            st.use { it.execute() }
        }
    }

}