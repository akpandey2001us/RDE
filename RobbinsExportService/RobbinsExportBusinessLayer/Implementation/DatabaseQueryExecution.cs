using Microsoft.Practices.EnterpriseLibrary.WindowsAzure.TransientFaultHandling.SqlAzure;
using Microsoft.Practices.TransientFaultHandling;
using System;
using System.Data;
using System.Data.SqlClient;

namespace RobbinsExportBusinessLayer
{
    /// <summary>
    /// Class to execute the database operations
    /// </summary>
	internal class DatabaseQueryExecution : IQueryExecution
	{
		private const int MaxRetry = 5;
		private const int DelayMs = 100;

		/// <summary>
		/// SQL connection retry
		/// </summary>
		private RetryPolicy SQLRetryConnection
		{
			get
			{
				var fromMilliseconds = TimeSpan.FromMilliseconds(DelayMs);
				var policy = new RetryPolicy<SqlAzureTransientErrorDetectionStrategy>
					(MaxRetry, fromMilliseconds);
				return policy;
			}
		}

		/// <summary>
		/// Execute SQL command
		/// </summary>
		/// <param name="query">Query string</param>
		/// <param name="tableName">Table name</param>
		/// <param name="connectionString">Connection string</param>
		/// <returns>DataTable object</returns>
		public DataTable ExecuteSQLCommand(string query, string tableName, string connectionString)
		{
			using (SqlConnection cnn = new SqlConnection
				  (connectionString))
			{
				return SQLRetryConnection.ExecuteAction(() =>
				{
					SqlCommand cmd =
					new SqlCommand(query, cnn);

					SqlDataAdapter da = new SqlDataAdapter(cmd);

					DataTable dt = new DataTable(tableName);

					da.Fill(dt);

					return dt;
				});
			}
		}

		/// <summary>
		/// Execute SQL command
		/// </summary>
		/// <param name="query">Sql query string</param>
		/// <param name="connectionString">connection string</param>
		/// <returns>number of rows impacted by query</returns>
		public int ExecuteSQLCommand(string query, string connectionString)
		{
			using (SqlConnection cnn = new SqlConnection
				  (connectionString))
			{
				return SQLRetryConnection.ExecuteAction(() =>
				{
					SqlCommand cmd =
					   new SqlCommand(query, cnn);

                    try
                    {
                        cnn.Open();
                        return cmd.ExecuteNonQuery();
                    }
                    finally
                    {
                        cnn.Close();
                    }
				});
			}
		}
	}
}
