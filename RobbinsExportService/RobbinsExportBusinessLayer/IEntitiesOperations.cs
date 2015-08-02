using System.Collections.Generic;
using System.Data;

namespace RobbinsExportBusinessLayer
{
    interface IEntitiesOperations
    {
        List<string> GetAllEntities();
        bool IsTableExists(string tableName, string connectionString);
        void CreateEntity(string tableName);
        DataTable GetEntityWithData(string tableName, string loadType = "F");
        bool TruncateEntity(string tableName);
    }
}
