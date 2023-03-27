package org.sunbird.obsrv.core.util

import java.sql.{Connection, ResultSet, SQLException, Statement}
import org.postgresql.ds.PGSimpleDataSource

final case class PostgresConnectionConfig(user: String, password: String, database: String, host: String, port: Int, maxConnections: Int)

class PostgresConnect(config: PostgresConnectionConfig) {

  private var source: PGSimpleDataSource = null

  buildPoolConfig()

  private var connection: Connection = source.getConnection
  private var statement: Statement = connection.createStatement

  @throws[Exception]
  private def buildPoolConfig(): Unit = {
    Class.forName("org.postgresql.Driver")
    source = new PGSimpleDataSource()
    source.setServerName(config.host)
    source.setPortNumber(config.port)
    source.setUser(config.user)
    source.setPassword(config.password)
    source.setDatabaseName(config.database)
  }

  @throws[Exception]
  def reset() = {
    closeConnection()
    buildPoolConfig()
    connection = source.getConnection
    statement = connection.createStatement
  }

  @throws[Exception]
  def closeConnection(): Unit = {
    connection.close()
  }

  @throws[Exception]
  def execute(query: String): Boolean = {
    try {
      statement.execute(query)
    }
    // $COVERAGE-OFF$ Disabling scoverage as the below code can only be invoked if postgres connection is stale
    catch {
      case ex: SQLException =>

        ex.printStackTrace() // TODO: Move this to system logs
        reset
        statement.execute(query)
    }
    // $COVERAGE-ON$
  }

  def executeQuery(query:String):ResultSet = statement.executeQuery(query)
}


