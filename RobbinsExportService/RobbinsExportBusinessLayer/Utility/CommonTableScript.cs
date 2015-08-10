using System.Data;
using System.Text;

namespace RobbinsExportBusinessLayer
{
    /// <summary>
    /// Common methods for creating script into database
    /// </summary>
    internal class CommonTableScript
    {
        private static DatabaseQueryExecution dbExecution;

        /// <summary>
        /// static constructor
        /// </summary>
        static CommonTableScript()
        {
            dbExecution = new DatabaseQueryExecution();
        }

        /// <summary>
        /// Create table
        /// </summary>
        /// <param name="tableName"></param>
        public static void CreateEntity(string tableName, string connectionString)
        {
            var tableDefinition = GetTableDefinition(tableName, connectionString);
            string sql = string.Format("exec sp_executesql {0}", tableDefinition);
            dbExecution.ExecuteSQLCommand(sql, connectionString);
        }

        /// <summary>
        /// Get the Primary key columns for table
        /// </summary>
        /// <param name="tableName">table name</param>
        /// <param name="connectionString">connection string</param>
        /// <returns>DataTable object</returns>
        public static DataTable GetPrimaryKeyColumns(string tableName, string connectionString)
        {
            string primaryKeyFinderSql = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(constraint_name), 'IsPrimaryKey') = 1 AND table_name = '" + tableName + "'";
            var tablePKDefn = dbExecution.ExecuteSQLCommand(primaryKeyFinderSql, tableName, connectionString);

            return tablePKDefn;
        }

        /// <summary>
        /// Get the Table definition script
        /// </summary>
        /// <param name="tableName">table name</param>
        /// <param name="connectionString">connection string</param>
        /// <returns>table script as string</returns>
        private static string GetTableDefinition(string tableName, string connectionString)
        {
            string sql = "SELECT ORDINAL_POSITION, COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '" + tableName + "'";
            var tableDefn = dbExecution.ExecuteSQLCommand(sql, tableName, connectionString);

            var tablePKDefn = GetPrimaryKeyColumns(tableName, connectionString);

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
    }
}
