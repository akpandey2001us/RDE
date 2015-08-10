using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace RobbinsExportBusinessLayer
{
    /// <summary>
    /// Data export business layer
    /// </summary>
	public class RobbinsExportServiceBusinessLayer :  IEntitiesOperations, IDisposable
	{
		private string SQLSourceDBConnectionString;
        private string SQLTargetDBConnectionString;
        private int loadCTVersion = 0;
        private DatabaseQueryExecution dbExecution = new DatabaseQueryExecution();

        /// <summary>
        /// Constuctor with initialization for source and destination database
        /// </summary>
        /// <param name="connectionStringForSourceDB">source database connection string</param>
        /// <param name="connectionStringForTargetDB">target database connection string</param>
		public RobbinsExportServiceBusinessLayer(string connectionStringForSourceDB, string connectionStringForTargetDB)
		{
            SQLSourceDBConnectionString = connectionStringForSourceDB;
            SQLTargetDBConnectionString = connectionStringForTargetDB;
		}

        /// <summary>
        /// Get all the database entities
        /// </summary>
        /// <returns>List of entities</returns>
        public List<string> GetAllEntities()
		{
			List<string> tableNames = new List<string>();

			string sql = "select * from sysobjects where xtype = 'U'";
            var tables = dbExecution.ExecuteSQLCommand(sql, "AllEntities", SQLSourceDBConnectionString);

			for (int i = 0; i < tables.Rows.Count; i++)
			{
				tableNames.Add(tables.Rows[i]["name"].ToString());
			}

			return tableNames;
		}

        /// <summary>
        /// Column mapping between source and destination database table
        /// </summary>
        /// <param name="sourceTableName">source table name</param>
        /// <param name="destinationTableName">destination table name</param>
        /// <returns>Dictionary object</returns>
        public Dictionary<string, string> GetColumns(string sourceTableName, string destinationTableName)
        {
            Dictionary<string,string> columnList = new Dictionary<string, string>();
            string sqlSource = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + sourceTableName + "'";
            var sqlSourceDataCols = dbExecution.ExecuteSQLCommand(sqlSource, sourceTableName, SQLSourceDBConnectionString);
            sqlSourceDataCols.Rows.Add("CDC_Type");

            string sqlDestination = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + destinationTableName + "'";
            var sqlDestinationDataCols = dbExecution.ExecuteSQLCommand(sqlDestination, sourceTableName, SQLTargetDBConnectionString);

            for (int i = 0; i < sqlSourceDataCols.Rows.Count; i++)
            {
                columnList.Add(sqlSourceDataCols.Rows[i][0].ToString(), sqlDestinationDataCols.Rows[i][0].ToString());
            }

            return columnList;
        }

        /// <summary>
        /// Verify if the table exists into database
        /// </summary>
        /// <param name="tableName">table name</param>
        /// <param name="connectionString">connection string</param>
        /// <returns>success flag</returns>
        public bool IsTableExists(string tableName, string connectionString)
		{
			string tableExistsSql = string.Format("SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{0}'", tableName);
            var result = dbExecution.ExecuteSQLCommand(tableExistsSql, "RowsCount", connectionString);
            return result.Rows.Count > 0;
		}

        public DataTable GetPrimaryKeyColumns(string tableName)
        {
            string primaryKeyFinderSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1 AND table_name = '" + tableName + "'";
            var tablePKDefn = dbExecution.ExecuteSQLCommand(primaryKeyFinderSql, tableName, SQLSourceDBConnectionString);

            return tablePKDefn;
        }

        private string GetTableDefinition(string tableName)
		{
			string sql = "SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
            var tableDefn = dbExecution.ExecuteSQLCommand(sql, tableName, SQLSourceDBConnectionString);

			var tablePKDefn = this.GetPrimaryKeyColumns(tableName);

			StringBuilder sb = new StringBuilder();

			sb.Append(string.Format("CREATE TABLE {0}(", tableName));
			for (int i = 0; i < tableDefn.Rows.Count; i++)
			{
				sb.Append(string.Format("{0} {1} {2},", tableDefn.Rows[i]["COLUMN_NAME"].ToString(), tableDefn.Rows[i]["DATA_TYPE"].ToString(), tableDefn.Rows[i]["IS_NULLABLE"].ToString() == "YES" ? "NOT NULL" : "NULL"));
			}
			sb.Append(string.Format("CONSTRAINT [PK_{0}] PRIMARY KEY CLUSTERED ( ", tableName));
			for (int i = 0; i < tablePKDefn.Rows.Count; i++)
			{
				sb.Append(string.Format("{0} ASC", tablePKDefn.Rows[i]["COLUMN_NAME"].ToString()));
				if (i > 0 && i != tablePKDefn.Rows.Count - 1)
				{
					sb.Append(",");
				}
			}
			sb.Append("))");

			return sb.ToString();
		}

        /// <summary>
        /// Get the table data to export
        /// Add additional column CDC_Type
        /// </summary>
        /// <param name="tableName">table name</param>
        /// <returns>DataTable object</returns>
		public DataTable GetEntityWithData(string tableName)
		{
			string sql = string.Format("select * from {0}", tableName);
            var result = dbExecution.ExecuteSQLCommand(sql, tableName, SQLSourceDBConnectionString);
            
            result.Columns.Add("CDC_Type");

            for (int i = 0; i < result.Rows.Count; i++)
            {
                result.Rows[i]["CDC_Type"] = "F";
            }
            
            return result;
		}

        /// <summary>
        /// Truncate data in table
        /// </summary>
        /// <param name="tableName">table name</param>
        /// <returns>success flag</returns>
        public bool TruncateEntity(string tableName)
		{
			string sql = string.Format("TRUNCATE TABLE {0}", tableName);
            return dbExecution.ExecuteSQLCommand(sql, SQLTargetDBConnectionString) > 0;
		}

        /// <summary>
        /// Create table
        /// </summary>
        /// <param name="tableName"></param>
        public void CreateEntity(string tableName)
        {
            var tableDefinition = GetTableDefinition(tableName);
            string sql = string.Format("exec sp_executesql {0}", tableDefinition);
            dbExecution.ExecuteSQLCommand(sql, SQLTargetDBConnectionString);
        }

        /// <summary>
        /// Insert a row into LoadStatusLog table
        /// </summary>
        /// <returns>return the inserted id</returns>
        public int InsertLoadStatusAndReturnId()
        {
            int loadFirstCTVersion = -1;
            DateTime loadFromDateTime = DateTime.UtcNow;
            int load_lastCTVersion = -1;
            DateTime loadToDateTime = DateTime.UtcNow;
            char loadTypeCode = 'D';

            var lastRecord = dbExecution.ExecuteSQLCommand("select top 1 * from LoadStatusLog order by load_Id desc", "LoadStatusLog", SQLTargetDBConnectionString);

            var lastCTVersion = Convert.ToInt32(dbExecution.ExecuteSQLCommand("select CHANGE_TRACKING_CURRENT_VERSION()", "LastCTVersion", SQLSourceDBConnectionString).Rows[0][0].ToString());

            if (lastRecord.Rows.Count > 0 && (lastRecord.Rows[0]["Load_Status_Code"].ToString() == "B" || lastRecord.Rows[0]["Load_Status_Code"].ToString() == "F"))
            {
                loadFirstCTVersion = Convert.ToInt32(lastRecord.Rows[0]["Load_First_CT_Version"].ToString());
                loadFromDateTime = Convert.ToDateTime(lastRecord.Rows[0]["Load_From_Datetime"].ToString());
                load_lastCTVersion = lastCTVersion;
                loadToDateTime = DateTime.UtcNow;
                loadTypeCode = 'D';
                this.loadCTVersion = Convert.ToInt32(lastRecord.Rows[0]["Load_First_CT_Version"].ToString());
            }
            
            if (lastRecord.Rows.Count > 0 && lastRecord.Rows[0]["Load_Status_Code"].ToString() == "S")
            {
                loadFirstCTVersion = Convert.ToInt32(lastRecord.Rows[0]["Load_Last_CT_Version"].ToString());
                loadFromDateTime = Convert.ToDateTime(lastRecord.Rows[0]["Load_To_Datetime"].ToString());
                load_lastCTVersion = lastCTVersion;
                loadToDateTime = DateTime.UtcNow;
                loadTypeCode = 'D';
                this.loadCTVersion = Convert.ToInt32(lastRecord.Rows[0]["Load_Last_CT_Version"].ToString());
            }

            if (lastRecord.Rows.Count == 0 || lastRecord.Rows[0]["Load_Status_Code"].ToString() == "I")
            {
                loadFirstCTVersion = -1;
                loadFromDateTime = DateTime.UtcNow;
                load_lastCTVersion = lastCTVersion;
                loadToDateTime = DateTime.UtcNow;
                this.loadCTVersion = -1;
                loadTypeCode = 'H';
            }

            var sql = string.Format("INSERT INTO LoadStatusLog([Load_First_CT_Version],[Load_From_Datetime],[Load_Last_CT_Version],[Load_To_Datetime],[Load_Status_Code],[Load_Type_Code]) VALUES({0},CONVERT(datetime,'{1}',103),{2},CONVERT(datetime,'{3}',103),'{4}','{5}')",
                this.loadCTVersion,
                loadFromDateTime,
                load_lastCTVersion,
                loadToDateTime,
                'P',
                loadTypeCode);

            dbExecution.ExecuteSQLCommand(sql, SQLTargetDBConnectionString);

            var result = dbExecution.ExecuteSQLCommand("select top 1 load_Id from LoadStatusLog order by load_Id desc", "LoadStatusLog", SQLTargetDBConnectionString);

            var loadId = result.Rows[0][0]
                .ToString();

            return Convert.ToInt32(loadId);
        }

        /// <summary>
        /// Update the LoadStatusLog table once load is completed or failed
        /// </summary>
        /// <param name="loadToDateTime">Load completed datetime</param>
        /// <param name="loadStatusCode">load status code</param>
        /// <param name="loadType">load type</param>
        /// <param name="load_Id">load Id</param>
        /// <returns>success flag</returns>
        public bool UpdateLoadStatusLog(
            DateTime loadToDateTime,
            char loadStatusCode,
            char loadType,
            int load_Id
            )
        {
            var sql = string.Format("UPDATE LoadStatusLog SET [Load_To_Datetime] = CONVERT(datetime,'{0}',103), [Load_Status_Code] = '{1}', [Load_Type_Code]='{2}' WHERE Load_Id = {3}",
                loadToDateTime,
                loadStatusCode,
                loadType,
                load_Id);

            return dbExecution.ExecuteSQLCommand(sql, SQLTargetDBConnectionString) > 0;
        }

        /// <summary>
        /// Get the last inserted column from LoadStatusLog table
        /// </summary>
        /// <param name="column">column name</param>
        /// <returns>string value for column</returns>
        public string GetLastInsertedColumnValueFromLoadStatusLogTable(string column)
        {
            string sql = "select top 1 " + column + " from LoadStatusLog order by Load_Id desc";
            var loadStatus = dbExecution.ExecuteSQLCommand(sql, "LoadStatusLog", SQLTargetDBConnectionString);
            
            return loadStatus.Rows.Count > 0 ? loadStatus.Rows[0][0].ToString() : null;
        }

        /// <summary>
        /// Get the tables with delta changes
        /// </summary>
        /// <returns></returns>
        public List<string> GetTablesWithValidDeltaChanges()
        {
            List<string> tables = new List<string>();
            string sql = string.Format("select name from sys.objects where object_id in (select object_Id from sys.change_tracking_tables where min_valid_version <= {0}) and [type] = 'U'", this.loadCTVersion == -1 ? 0 : this.loadCTVersion);
            var deltaTables = dbExecution.ExecuteSQLCommand(sql, "DeltaTables", SQLSourceDBConnectionString);
            for (int i = 0; i < deltaTables.Rows.Count; i++)
            {
                tables.Add(deltaTables.Rows[i][0].ToString());
            }

            return tables;
        }

        /// <summary>
        /// Get table with delta changes
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public DataTable GetTableDataWithDeltaChanges(string tableName)
        {
            var primaryKeys = this.GetPrimaryKeyColumns(tableName);
            StringBuilder queryBuilder = new StringBuilder();

            queryBuilder.Append("SELECT ");
            for (int i = 0; i < primaryKeys.Rows.Count; i++)
            {
                queryBuilder.Append("CT."+primaryKeys.Rows[i][0].ToString());
                queryBuilder.Append(",");
            }
            queryBuilder.Append("T.*,");
            queryBuilder.Append("CDC_TYPE = Case CT.SYS_CHANGE_OPERATION when 'I' then 'N' Else CT.SYS_CHANGE_OPERATION End");
            queryBuilder.Append(string.Format(" FROM CHANGETABLE(CHANGES [dbo].[{0}], {1})  CT", tableName, this.loadCTVersion));
            queryBuilder.Append(" left join ");
            queryBuilder.Append(string.Format("{0} T", tableName));
            queryBuilder.Append(" on ");

            for (int i = 0; i < primaryKeys.Rows.Count; i++)
            {
                queryBuilder.Append("CT." + primaryKeys.Rows[i][0].ToString() + " = " + " T." + primaryKeys.Rows[i][0].ToString());
                if(i != primaryKeys.Rows.Count - 1)
                queryBuilder.Append(" and ");
            }

            var result = dbExecution.ExecuteSQLCommand(queryBuilder.ToString(), "DeltaChanges", SQLSourceDBConnectionString);
            
            return result;
        }

        /// <summary>
        /// Decrypt the account table records
        /// </summary>
        /// <param name="accounts"></param>
        /// <param name="cert"></param>
        /// <returns></returns>
        public DataTable DecryptAccountRecords(DataTable accounts, string cert)
        {
            for (int i = 0; i < accounts.Rows.Count; i++)
            {
                accounts.Rows[i]["EmailAddress"] = DataDecryptionLayer.Decrypt(accounts.Rows[i]["EmailAddress"].ToString(), DataDecryptionLayer.GetCurrentCertificate(cert));
                accounts.Rows[i]["PhoneNumber"] = DataDecryptionLayer.Decrypt(accounts.Rows[i]["PhoneNumber"].ToString(), DataDecryptionLayer.GetCurrentCertificate(cert));
            }

            return accounts;
        }

        /// <summary>
        /// Dispose global objects
        /// </summary>
        public void Dispose()
        {
            SQLSourceDBConnectionString = null;
            SQLTargetDBConnectionString = null;
        }
    }
}
