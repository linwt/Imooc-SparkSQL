package utils

import java.sql.{Connection, DriverManager, PreparedStatement}

object MySQLUtil {

  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/imooc?user=root&password=123456")
  }

  def release(con: Connection, state: PreparedStatement): Unit = {
    try {
      if(state != null) {
        state.close()
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      if(con != null) {
        con.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    print(getConnection())
  }
}
