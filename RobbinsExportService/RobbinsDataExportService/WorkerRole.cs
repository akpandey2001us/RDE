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
        private int maxParallelThread = 1;

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

		public override bool OnStart()
		{
			// Set the maximum number of concurrent connections
			ServicePointManager.DefaultConnectionLimit = 12;

			// For information on handling configuration changes
			// see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.

			bool result = base.OnStart();

			Trace.TraceInformation("RobbinsDataExportService has been started");

			fullLoadTables.AddRange(CloudConfigurationManager.GetSetting("TablesForHistoricLoad").Split(new char[] { ',' }));
			deltaLoadTables.AddRange(CloudConfigurationManager.GetSetting("TablesForDeltaLoad").Split(new char[] { ',' }));

			// 1. Get all the sql server tables
			entities = businessLayer.GetAllEntities();

			var lastStatusCode = businessLayer.GetLastColumnValueFromLoadStatus("Load_Status_Code");

			if (lastStatusCode == "B" || lastStatusCode == "S")
			{
				this.DeltaLoad();
			}
			else
			{
				this.InitialLoad();
			}

			// 4. Start processing data for Delta load
			// 5. If OnStart method is called, then it will start with Historic load always

			return result;
		}

		public override void OnStop()
		{
			Trace.TraceInformation("RobbinsDataExportService is stopping");

			this.cancellationTokenSource.Cancel();
			this.runCompleteEvent.WaitOne();

			base.OnStop();

			Trace.TraceInformation("RobbinsDataExportService has stopped");
		}

		private async Task RunAsync(CancellationToken cancellationToken)
		{
			// TODO: Replace the following with your own logic. Convert.ToInt32(CloudConfigurationManager.GetSetting("HistoricLoadInterval"))
			while (!cancellationToken.IsCancellationRequested)
			{
				Trace.TraceInformation("Working");
				await Task.Delay(10000);

				if (this.TimeDiff() == 10)
				{
					Trace.WriteLine("Running historic load at " + DateTime.Now);
					var lastStatusCode = businessLayer.GetLastColumnValueFromLoadStatus("Load_Status_Code");

					if (lastStatusCode == "B" || lastStatusCode == "S")
					{
						this.DeltaLoad();
					}
					else
					{
						this.InitialLoad();
					}
				}

				// 1. Run Historic load on ad-hoc basis
				// 2. Start processing data for Delta load            
			}
		}

		private int TimeDiff(string loadType = "historic")
		{
			TimeSpan varTime = DateTime.Now - businessLayer.GetLastHistoricLoadDateTime();
			return varTime.Minutes;
		}

		private void InitialLoad()
		{
			// Log into Load Status table
			int loadId = businessLayer.InsertLoadStatusAndReturnId(null, DateTime.Now, null, null, 'P', 'F');

			// ready to load the data
			businessLayer.UpdateLoadStatusLog(DateTime.Now, 'R', 'F', loadId);

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
				businessLayer.UpdateLoadStatusLog(DateTime.Now, 'F', 'H', loadId);
			}

			// 3. Update the load_status_details table
			businessLayer.UpdateLoadStatusLog(DateTime.Now, 'S', 'H', loadId);
		}

		private void DeltaLoad()
		{
			var load_First_CT_Version = Convert.ToInt32(businessLayer.GetLastColumnValueFromLoadStatus("Load_First_CT_Version"));
			var validDeltaTables = businessLayer.GetTablesWithValidDeltaChanges();
            bool delta = false;

			// Log into Load Status table
			int loadId = businessLayer.InsertLoadStatusAndReturnId(null, DateTime.Now, null, null, 'P', 'F');

			// ready to load the data
			businessLayer.UpdateLoadStatusLog(DateTime.Now, 'R', 'D', loadId);

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
				businessLayer.UpdateLoadStatusLog(DateTime.Now, 'F', 'D', loadId);
			}

			// 3. Update the load_status_details table
			businessLayer.UpdateLoadStatusLog(DateTime.Now, 'S', delta == true ? 'D': 'F', loadId);
		}
	}
}
