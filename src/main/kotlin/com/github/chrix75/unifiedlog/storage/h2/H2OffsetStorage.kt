package com.github.chrix75.unifiedlog.storage.h2

import csperandio.unifiedlog.storage.OffsetStorage
import java.sql.Connection
import javax.sql.DataSource

class H2OffsetStorage(
    private val ds: DataSource,
    private val clientId: String
) : OffsetStorage {

    private var current: Int = loadLastCurrent(ds, clientId)

    private fun loadLastCurrent(ds: DataSource, clientId: String): Int {
        createTableIfNotExists()
        return ds.connection.use { c ->
            c.prepareStatement("SELECT OFFSET_VALUE FROM WALKER_OFFSET WHERE CLIENT_ID = ?").use { st ->
                st.setString(1, clientId)
                st.executeQuery().use {
                    var last = 0
                    if (it.next()) {
                        last = it.getInt(1)
                    }

                    last
                }
            }
        }
    }

    override val value: Int
        get() = current

    override operator fun inc(): H2OffsetStorage {
        ++current
        saveCurrentOffset()
        return this
    }

    private fun saveCurrentOffset() {
        ds.connection.use { c ->
            updateOffset(c)
        }
    }

    private fun updateOffset(c: Connection) {
        c.prepareStatement(
            "UPDATE WALKER_OFFSET SET OFFSET_VALUE = ? WHERE CLIENT_ID = ?"
        ).use { st ->
            st.setInt(1, current)
            st.setString(2, clientId)
            if (st.executeUpdate() == 0) {
                saveNewOffset(c)
            }
        }
    }

    private fun saveNewOffset(c: Connection) {
        c.prepareStatement(
            "INSERT INTO WALKER_OFFSET(CLIENT_ID, OFFSET_VALUE) " +
                    "VALUES (?, ?)"
        ).use { st ->
            st.setString(1, clientId)
            st.setInt(2, current)
            st.executeUpdate()
        }
    }

    private fun createTableIfNotExists() {
        ds.connection.use { c ->
            c.prepareStatement(
                "CREATE TABLE IF NOT EXISTS WALKER_OFFSET (" +
                        "CLIENT_ID VARCHAR2(50) PRIMARY KEY," +
                        "OFFSET_VALUE INT)"
            ).use { it.execute() }
        }
    }
}