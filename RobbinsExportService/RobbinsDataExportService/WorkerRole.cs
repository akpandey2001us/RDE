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
		private List<string> deltaLoadTables = new List<string>();
		private List<string> entities = new List<string>();
        private int maxParallelThread = -1;

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

            this.maxParallelThread = Convert.ToInt32(CloudConfigurationManager.GetSetting("MaxParallelProcessCount"));

			fullLoadTables.AddRange(CloudConfigurationManager.GetSetting("TablesForHistoricLoad").Split(new char[] { ',' }));
			deltaLoadTables.AddRange(CloudConfigurationManager.GetSetting("TablesForDeltaLoad").Split(new char[] { ',' }));

			// 1. Get all the sql server tables
			entities = businessLayer.GetAllEntities();

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
        /// Data export method
        /// </summary>
        private void RunDataExport()
        {
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
			businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'P', 'F', loadId);

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
						// TODO: need to check the performance 
						var entityData = businessLayer.GetEntityWithData(entity);

						if (entity == "Accounts")
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
			catch (Exception)
			{
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
            bool delta = false;

			// ready to load the data
			businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'P', 'D', loadId);

			try
			{
				// 2. Start processing data for Historic load into replica database
				Parallel.ForEach(deltaLoadTables, new ParallelOptions() { MaxDegreeOfParallelism = maxParallelThread }, (entity) =>
				{
					// truncate all the existing data to avoid duplication
					if (businessLayer.IsTableExists(entity, CloudConfigurationManager.GetSetting("TargetDatabaseConnectionString")))
					{
						Trace.WriteLine(string.Format("Truncating data for entity {0}", entity));

						businessLayer.TruncateEntity(entity);

						Trace.WriteLine(string.Format("Processing data for entity {0}", entity));

						// pull all the data into in-memory datatable
						// TODO: need to check the performance 
						DataTable entityData = new DataTable();

						if (validDeltaTables.Contains(entity))
						{
							// get delta changes
							entityData = businessLayer.GetTableDataWithDeltaChanges(entity);
                            delta = true;
						}
						else
						{
							// get full load, no valid delta for this entity
							entityData = businessLayer.GetEntityWithData(entity);
						}

						if (entity == "Accounts")
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
			catch (Exception)
			{
				businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'F', 'D', loadId);
			}

			// 3. Update the load_status_details table
			businessLayer.UpdateLoadStatusLog(DateTime.UtcNow, 'R', delta == true ? 'D': 'H', loadId);
        }

        #endregion
    }
}
