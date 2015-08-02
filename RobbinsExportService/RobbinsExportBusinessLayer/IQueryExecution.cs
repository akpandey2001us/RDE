using System.Data;

namespace RobbinsExportBusinessLayer
{
    interface IQueryExecution
    {
        DataTable ExecuteSQLCommand(string query, string tableName, string connectionString);
        int ExecuteSQLCommand(string query, string connectionString);
    }
}
