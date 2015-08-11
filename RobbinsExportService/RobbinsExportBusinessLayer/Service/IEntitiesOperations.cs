using System;
using System.Collections.Generic;
using System.Data;

namespace RobbinsExportBusinessLayer
{
	interface IEntitiesOperations
	{
		List<string> GetAllEntities();
		bool IsTableExists(string tableName, string connectionString);
		DataTable GetEntityWithData(string tableName);
		bool TruncateEntity(string tableName);
		List<string> GetTablesWithValidDeltaChanges();
		DataTable GetTableDataWithDeltaChanges(string tableName);
		int InsertLoadStatusAndReturnId();
		bool UpdateLoadStatusLog(char loadStatusCode, char loadType, int load_Id);
		string GetLastInsertedColumnValueFromLoadStatusLogTable(string column);
	}
}
