using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;

namespace AFSDK_CustomDR_SQL
{
    /*****************************************************************************************
     * SQL Helper class to get data from the specified SQL Server, SQL Database and SQL Table. 
     * Given a start and end time, the static GetSQLData method will return a SQLDataReader
     * object that contains all values between the time interval in ascending order.
     * Given the end time only, only the most current value will be returned. 
     *****************************************************************************************/
    class SQLHelper
    {
        public static SqlDataReader GetSQLData(string sqlServer, string sqlDb, string sqlTable, DateTime startTime, DateTime endTime)
        {
            // Construct connection string to SQL Server based on input parameters for SQL server name and database name.
            string connectString = String.Format("server={0}; database={1}; Integrated Security=SSPI; Connection Timeout=10", sqlServer, sqlDb);
            SqlConnection sqlConnection = new SqlConnection(connectString);

            // Construct SQL query
            string query;
            using (SqlCommand cmd = new SqlCommand())
            {
                cmd.Connection = sqlConnection;

                // SQL query for the most recent values before the end time
                if (startTime == DateTime.MinValue)
                {
                    query = String.Format("SELECT TOP 1 pi_time, pi_value FROM {0}.{1} WHERE pi_time <= @time ORDER BY pi_time DESC", sqlDb, sqlTable);
                    SqlParameter sqlTime = cmd.Parameters.Add(new SqlParameter("time", System.Data.SqlDbType.DateTime2));
                    sqlTime.Value = endTime;
                    cmd.CommandText = query;
                }

                // SQL query for all values over a specified time range
                else
                {
                    query = String.Format("SELECT pi_time, pi_value FROM {0}.{1} WHERE pi_time >= @startTime AND pi_time <= @endTime ORDER BY pi_time ASC", sqlDb, sqlTable);
                    SqlParameter sqlStartTime = cmd.Parameters.Add(new SqlParameter("startTime", System.Data.SqlDbType.DateTime2));
                    SqlParameter sqlEndTime = cmd.Parameters.Add(new SqlParameter("endTime", System.Data.SqlDbType.DateTime2));
                    sqlStartTime.Value = startTime;
                    sqlEndTime.Value = endTime;
                    cmd.CommandText = query;
                }

                /* Open SQL connection and return the SqlDataReader object. Use CommandBehavior.CloseConnection to ensure that the 
                 * SQL connection is closed when the SqlDataReader object is closed. */
                sqlConnection.Open();
                SqlDataReader sqlReader = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return sqlReader;
            }
        }
    }
}
