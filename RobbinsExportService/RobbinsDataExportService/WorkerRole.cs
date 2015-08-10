using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.ServiceRuntime;
using RobbinsExportBusinessLayer;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace RobbinsDataExportService
{
    /// <summary>
    /// Worker Role class
    /// This will export the data from Robbins database to Robbins replica database
    /// The load status will be P(Preparing) and then R(Ready) after successful data export
    /// In case it fails, then the final load Status will be F(Fail)
    /// </summary>
	public class WorkerRole : RoleEntryPoint
	{
		private readonly CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();
		private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
		private RobbinsExportServiceBusinessLayer businessLayer = new RobbinsExportServiceBusinessLayer
			(
				CloudConfigurationManager.GetSetting("SourceDatabaseConnectionString"),
				CloudConfigurationManager.GetSetting("TargetDatabaseConnectionString")
			);
		private List<string> fullLoadTables = new List<string>();
        private List<string> referenceTables = new List<string>();
		private List<string> transactionTables = new List<string>();
		private List<string> entities = new List<string>();
        private int maxParallelThread = 0;
        /// <summary>
        /// Method to called always while the Role instance is running
        /// </summary>
		public override void Run()
		{
			Trace.TraceInformation("RobbinsDataExportService is running");

			try
			{
				this.RunAsync(this.cancellationTokenSource.Token).Wait();
			}
			finally
			{
				this.runCompleteEvent.Set();
			}
		}

        /// <summary>
        /// Starting method to be called on Role initialization or restart
        /// </summary>
        /// <returns></returns>
		public override bool OnStart()
		{
			// Set the maximum number of concurrent connections
			ServicePointManager.DefaultConnectionLimit = 12;

			bool result = base.OnStart();

			Trace.TraceInformation("RobbinsDataExportService has been started");

            this.PrepareDataExport();

            this.RunDataExport();

			// 4. Start processing data for Delta load
			// 5. If OnStart method is called, then it will start with Historic load always

			return result;
		}

        /// <summary>
        /// Method to be called in case Worker Role gets stopped
        /// </summary>
		public override void OnStop()
		{
			Trace.TraceInformation("RobbinsDataExportService is stopping");

			this.cancellationTokenSource.Cancel();
			this.runCompleteEvent.WaitOne();

			base.OnStop();

			Trace.TraceInformation("RobbinsDataExportService has stopped");
		}

        #region Private Members

        /// <summary>
        /// Prepare the data export operation
        /// </summary>
        private void PrepareDataExport()
        {
            this.maxParallelThread = Convert.ToInt32(CloudConfigurationManager.GetSetting("MaxParallelProcessCount"));

            fullLoadTables.Clear();
            referenceTables.Clear();
            transactionTables.Clear();

            fullLoadTables.AddRange(CloudConfigurationManager.GetSetting("TablesForHistoricLoad").Split(new char[] { ',' }));
            referenceTables.AddRange(CloudConfigurationManager.GetSetting("ReferenceTables").Split(new char[] { ',' }));
            transactionTables.AddRange(CloudConfigurationManager.GetSetting("TransactionTables").Split(new char[] { ',' }));

            // 1. Get all the sql server tables
            entities = businessLayer.GetAllEntities();
        }

        /// <summary>
        /// Data export method
        /// </summary>
        private void RunDataExport()
        {
            this.PrepareDataExport();

            var lastStatusCode = businessLayer.GetLastInsertedColumnValueFromLoadStatusLogTable("Load_Status_Code");

            switch (lastStatusCode)
            {
                case "B":
                case "S":
                    this.DeltaLoad();
                    break;

                case null:
                case "I":
                    this.InitialLoad();
                    break;
            }
        }

        /// <summary>
        /// Asynchronous execution for Run method
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns>async Task thread object</returns>
		private async Task RunAsync(CancellationToken cancellationToken)
		{
			while (!cancellationToken.IsCancellationRequested)
			{
				Trace.TraceInformation("Working");
                await Task.Delay(Convert.ToInt32(CloudConfigurationManager.GetSetting("RoleInterval")));
                
				Trace.WriteLine("Running historic load at " + DateTime.Now);
                
                this.RunDataExport();

				// 1. Run Historic load on ad-hoc basis
				// 2. Start processing data for Delta load            
			}
		}

        /// <summary>
        /// Initial Load from Robbins database to Robbins replica
        /// </summary>
		private void InitialLoad()
		{
			// Log into Load Status table
			int loadId = businessLayer.InsertLoadStatusAndReturnId();

			// ready to load the data
			businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'P', 'H', loadId);

			try
			{
				// disable indexes
				// businessLayer.DisableIndexes();

				// 2. Start processing data for Historic load into replica database
				Parallel.ForEach(entities, new ParallelOptions() { MaxDegreeOfParallelism = maxParallelThread }, (entity) =>
				{
					// truncate all the existing data to avoid duplication
					if (businessLayer.IsTableExists(entity, CloudConfigurationManager.GetSetting("TargetDatabaseConnectionString")) && fullLoadTables.Contains(entity))
					{
						Trace.WriteLine(string.Format("Truncating data for entity {0}", entity));

						businessLayer.TruncateEntity(entity);

						Trace.WriteLine(string.Format("Processing data for entity {0}", entity));

						// pull all the data into in-memory datatable
						var entityData = businessLayer.GetEntityWithData(entity);

						if (entity == "Accounts" && CloudConfigurationManager.GetSetting("IsAccountDecryptionReqd")=="1")
						{
							entityData = businessLayer.DecryptAccountRecords(entityData, CloudConfigurationManager.GetSetting("EncryptionCert"));
						}

						// write data to destination table
						BulkWriter tableWriter = new BulkWriter(entity, businessLayer.GetColumns(entity, entity));
						tableWriter.WriteWithRetries(entityData);

						Trace.WriteLine(string.Format("Data written for entity {0}", entity));
					}
				});
			}
			catch (Exception ex)
			{
                Trace.WriteLine(ex.StackTrace, "Error");
				businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'F', 'H', loadId);
			}

			// 3. Update the load_status_details table
			businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'R', 'H', loadId);
		}

        /// <summary>
        /// Data load with delta changes
        /// </summary>
		private void DeltaLoad()
		{
			// Log into Load Status table and set delta version
			int loadId = businessLayer.InsertLoadStatusAndReturnId();

            // get the tables with valid delta version
            var validDeltaTables = businessLayer.GetTablesWithValidDeltaChanges();

			// ready to load the data
			businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'P', 'D', loadId);

			try
			{
				// 2. Start processing data for Historic load into replica database
				Parallel.ForEach(entities, new ParallelOptions() { MaxDegreeOfParallelism = maxParallelThread }, (entity) =>
				{
					if (businessLayer.IsTableExists(entity, CloudConfigurationManager.GetSetting("TargetDatabaseConnectionString")))
					{
						// pull all the data into in-memory datatable
						DataTable entityData = new DataTable();

                        this.TruncateEntityData(entity);

                        Trace.WriteLine(string.Format("Processing data for entity {0}", entity));

						if (transactionTables.Contains(entity) && validDeltaTables.Contains(entity))
						{
							// get delta changes
							entityData = businessLayer.GetTableDataWithDeltaChanges(entity);
						}

					    // Reference tables need to copy with full data
                        // The table set should be mutually exclusive between Reference tables and Transaction tables
                        if(referenceTables.Contains(entity))
						{
							// get full load, no valid delta for this entity
							entityData = businessLayer.GetEntityWithData(entity);
						}

                        // decrypt the account data
						if (entity == "Accounts" && entityData.Rows.Count > 0 && CloudConfigurationManager.GetSetting("IsAccountDecryptionReqd")=="1")
						{
							entityData = businessLayer.DecryptAccountRecords(entityData, CloudConfigurationManager.GetSetting("EncryptionCert"));
						}

                        if (entityData.Rows.Count > 0)
                        {
                            // write data to destination table
                            BulkWriter tableWriter = new BulkWriter(entity, businessLayer.GetColumns(entity, entity));
                            tableWriter.WriteWithRetries(entityData);

                            Trace.WriteLine(string.Format("Data written for entity {0}", entity));
                        }
					}
				});
			}
			catch (Exception ex)
			{
                Trace.WriteLine(ex.StackTrace, "Error");
				businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'F', 'D', loadId);
			}

			// 3. Update the load_status_details table
			businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'R', validDeltaTables.Count > 0 ? 'D' : 'H', loadId);
        }

        /// <summary>
        /// Truncate the entity data
        /// </summary>
        /// <param name="tableName">table name</param>
        private void TruncateEntityData(string tableName)
        {
            Trace.WriteLine(string.Format("Truncating data for entity {0}", tableName));

            businessLayer.TruncateEntity(tableName);
        }

        #endregion
    }
}
