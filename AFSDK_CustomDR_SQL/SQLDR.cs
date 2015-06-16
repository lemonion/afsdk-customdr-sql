using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OSIsoft.AF.Asset;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Data;
using System.Data.SqlClient;
using OSIsoft.AF.Time;
using OSIsoft.AF.Data;

namespace AFSDK_CustomDR_SQL
{
    /******************************************************************************
     * A custom data reference for the PI AF server that retrieves the timestamps and
     * values from a specified SQL table. 
     *****************************************************************************/
    [Serializable]
    [Guid("A1AC3A39-9E55-4700-BBC0-68299E67C4A1")]
    [Description("SQLDR; Get values from a SQL table")]
    public class SQLDR : AFDataReference
    {
        // Private fields storing configuration of data reference
        private string _tableName = String.Empty;
        private string _dbName = String.Empty;
        private string _sqlName = String.Empty;

        // Public property for name of the SQL table
        public string TableName
        {
            get
            {
                return _tableName;
            }
            set
            {
                if (_tableName != value)
                {
                    _tableName = value;
                    SaveConfigChanges();
                }
            }
        }

        // Public property for name of the SQL database
        public string DBName
        {
            get
            {
                return _dbName;
            }
            set
            {
                if (_dbName != value)
                {
                    _dbName = value;
                    SaveConfigChanges();
                }
            }
        }

        // Public property for name of the SQL instance
        public string SQLName
        {
            get
            {
                return _sqlName;
            }
            set
            {
                if (_sqlName != value)
                {
                    _sqlName = value;
                    SaveConfigChanges();
                }
            }
        }

        // Get or set the config string for the SQL data reference
        public override string ConfigString
        {
            get
            {
                return String.Format("{0};{1};{2}", SQLName, DBName, TableName);
            }
            set
            {
                if (value != null)
                {
                    string[] configSplit = value.Split(';');
                    SQLName = configSplit[0];
                    DBName = configSplit[1];
                    TableName = configSplit[2];
                    SaveConfigChanges();
                }
            }
        }

        // Return latest value if timeContext is null, otherwise return latest value before a specific time
        public override AFValue GetValue(object context, object timeContext, AFAttributeList inputAttributes, AFValues inputValues)
        {
            AFValue currentVal = new AFValue();
            DateTime time;
            if (timeContext != null)
            {
                time = ((AFTime)timeContext).LocalTime;
            }
            else
            {
                time = DateTime.Now;
            }
            using (SqlDataReader reader = SQLHelper.GetSQLData(SQLName, DBName, TableName, DateTime.MinValue, time))
            {
                if (reader.Read())
                {
                    currentVal.Timestamp = AFTime.Parse(reader["pi_time"].ToString());
                    currentVal.Value = reader["pi_value"];
                }
            }

            return currentVal;
        }

        // Return all values (converted to AFValues) over a specific time interval
        public override AFValues GetValues(object context, AFTimeRange timeRange, int numberOfValues, AFAttributeList inputAttributes, AFValues[] inputValues)
        {
            AFValues values = new AFValues();
            DateTime startTime = timeRange.StartTime.LocalTime;
            DateTime endTime = timeRange.EndTime.LocalTime;
            using (SqlDataReader reader = SQLHelper.GetSQLData(SQLName, DBName, TableName, startTime, endTime))
            {
                while (reader.Read())
                {
                    AFValue newVal = new AFValue();
                    newVal.Timestamp = AFTime.Parse(reader["pi_time"].ToString());
                    newVal.Value = reader["pi_value"];
                    values.Add(newVal);
                }
            }
            return values;
        }

        // Return an AFEventSource object for this custom data reference
        public static object CreateDataPipe()
        {
            EventSource pipe = new EventSource();
            return pipe;
        }

        public override AFDataReferenceMethod SupportedMethods
        {
            get
            {
                return AFDataReferenceMethod.GetValue | AFDataReferenceMethod.GetValues;
            }
        }

        public override AFDataMethods SupportedDataMethods
        {
            get
            {
                return AFDataMethods.DataPipe;
            }
        }

        public override AFDataReferenceContext SupportedContexts
        {
            get
            {
                return AFDataReferenceContext.Time;
            }
        }



    }
}
