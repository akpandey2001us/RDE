using Microsoft.Practices.EnterpriseLibrary.WindowsAzure.TransientFaultHandling.SqlAzure;
using Microsoft.Practices.TransientFaultHandling;
using Microsoft.WindowsAzure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;

namespace RobbinsDataExportService
{
    /// <summary>
    /// Bulk writer class
    /// </summary>
    public class BulkWriter
    {
        const int MaxRetry = 5;
        const int DelayMs = 100;

        private readonly string tableName;
        private readonly Dictionary<string, string> tableMap;
        private readonly string connString;

        public BulkWriter(string tableName,
                                    Dictionary<string, string> tableMap)
        {
            this.tableName = tableName;
            this.tableMap = tableMap;

            // get target database connection string
            connString = CloudConfigurationManager
                .GetSetting("TargetDatabaseConnectionString");
        }

        /// <summary>
        /// Write with retry
        /// </summary>
        /// <param name="datatable">datatable object</param>
        public void WriteWithRetries(DataTable datatable)
        {
            TryWrite(datatable);
        }

        /// <summary>
        /// Write data into server
        /// </summary>
        /// <param name="datatable">DataTable object</param>
        private void TryWrite(DataTable datatable)
        {
            var policy = MakeRetryPolicy();
            try
            {
                policy.ExecuteAction(() => Write(datatable));
            }
            catch (Exception ex)
            {
                //logging
                Trace.TraceError(ex.ToString());
                throw new Exception(ex.Message);
            }
        }

        /// <summary>
        /// Write the data table
        /// </summary>
        /// <param name="datatable">DataTable object</param>
        private void Write(DataTable datatable)
        {
            // connect to SQL
            using (var connection =
                new SqlConnection(connString))
            {
                var bulkCopy = MakeSqlBulkCopy(connection);

                // write into destination table
                connection.Open();

                using (var dataTableReader = new DataTableReader(datatable))
                {
                    bulkCopy.WriteToServer(dataTableReader);
                }

                connection.Close();
            }
        }

        /// <summary>
        /// Retry policy
        /// </summary>
        /// <returns>RetryPolicy object</returns>
        private RetryPolicy<SqlAzureTransientErrorDetectionStrategy> MakeRetryPolicy()
        {
            var fromMilliseconds = TimeSpan.FromMilliseconds(DelayMs);
            var policy = new RetryPolicy<SqlAzureTransientErrorDetectionStrategy>
                (MaxRetry, fromMilliseconds);
            return policy;
        }

        /// <summary>
        /// Create SQL Bulk copy
        /// </summary>
        /// <param name="connection">connection object</param>
        /// <returns>SQL Bulk copy object</returns>
        private SqlBulkCopy MakeSqlBulkCopy(SqlConnection connection)
        {
            var bulkCopy =
                new SqlBulkCopy
                    (
                    connection,
                    SqlBulkCopyOptions.TableLock |
                    SqlBulkCopyOptions.FireTriggers |
                    SqlBulkCopyOptions.UseInternalTransaction,
                    null
                    )
                {
                    DestinationTableName = tableName,
                    EnableStreaming = true,
                    BulkCopyTimeout = 0
                };

            tableMap
                .ToList()
                .ForEach(kp =>
                {
                    bulkCopy
                .ColumnMappings
                .Add(kp.Key, kp.Value);
                });

            return bulkCopy;
        }
    }
}
