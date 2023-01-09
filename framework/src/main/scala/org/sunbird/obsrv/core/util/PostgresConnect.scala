package org.sunbird.obsrv.core.util

import java.sql.{Connection, ResultSet, SQLException, Statement}
import org.postgresql.ds.PGPoolingDataSource

final case class PostgresConnectionConfig(user: String, password: String, database: String, host: String, port: Int, maxConnections: Int)

class PostgresConnect(config: PostgresConnectionConfig) {

  private var source: PGPoolingDataSource = null

  buildPoolConfig()

  var connection: Connection = source.getConnection
  private var statement: Statement = connection.createStatement


  @throws[Exception]
  private def buildPoolConfig(): Unit = {
    Class.forName("org.postgresql.Driver")
    source = new PGPoolingDataSource()
    source.setServerName(config.host)
    source.setPortNumber(config.port)
    source.setUser(config.user)
    source.setPassword(config.password)
    source.setDatabaseName(config.database)
    source.setMaxConnections(config.maxConnections)
  }

  @throws[Exception]
  def resetConnection: Connection = {
    closeConnection()
    buildPoolConfig()
    connection = source.getConnection
    statement = connection.createStatement
    connection
  }

  def getConnection: Connection = this.connection

  @throws[Exception]
  def closeConnection(): Unit = {
    connection.close()
    source.close()
  }

  @throws[Exception]
  def execute(query: String): Boolean = try statement.execute(query)
  catch {
    case ex: SQLException =>
      ex.printStackTrace()
      resetConnection
      statement.execute(query)
  }

  def executeQuery(query:String):ResultSet = statement.executeQuery(query)
}


