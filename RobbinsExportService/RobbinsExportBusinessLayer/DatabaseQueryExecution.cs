﻿using Microsoft.Practices.EnterpriseLibrary.WindowsAzure.TransientFaultHandling.SqlAzure;
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
		const int MaxRetry = 5;
		const int DelayMs = 100;

		/// <summary>
		/// SQL retry
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
		/// <param name="query"></param>
		/// <param name="tableName"></param>
		/// <param name="connectionString"></param>
		/// <returns></returns>
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
		/// <param name="query"></param>
		/// <param name="connectionString"></param>
		/// <returns></returns>
		public int ExecuteSQLCommand(string query, string connectionString)
		{
			using (SqlConnection cnn = new SqlConnection
				  (connectionString))
			{
				return SQLRetryConnection.ExecuteAction(() =>
				{
					SqlCommand cmd =
					   new SqlCommand(query, cnn);

					cnn.Open();

					var result = cmd.ExecuteNonQuery();

					cnn.Close();

					return result;
				});
			}
		}
	}
}
