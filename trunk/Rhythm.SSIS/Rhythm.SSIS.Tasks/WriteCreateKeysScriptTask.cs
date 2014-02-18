﻿using System;
using System.ComponentModel;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Dts.Runtime;

namespace Rhythm.SSIS.Tasks
{
    [DtsTask(
        DisplayName = "Write Create Keys Script",
        Description = "Writes a SQL script to the specified file that re-creates foriegn keys for the given connection.")]
    public class WriteCreateKeysScriptTask : Task
    {
        private const string SELECT_STATEMENT_PLACEHOLDER = "{SELECT_STATEMENT_PLACEHOLDER}";

        [Category("Write Create Keys Script")]
        public string ConnectionName { get; set; }

        [Category("Write Create Keys Script")]
        public string TargetPath { get; set; }

        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log)
        {
            return (
                connections.Contains(ConnectionName)
                && !string.IsNullOrWhiteSpace(TargetPath)
                ) ? DTSExecResult.Success : DTSExecResult.Failure;
        }

        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction) {
            try {
                var connectionManager = connections[ConnectionName];
                var connection = connectionManager.AcquireConnection(transaction) as SqlConnection;

                var dropKeysScript = Resources.DropCreateFKScriptCommon.Replace(SELECT_STATEMENT_PLACEHOLDER, Resources.CreateFKScript);

                var command = new SqlCommand(dropKeysScript, connection);

                var reader = command.ExecuteReader();

                var scriptBuilder = new StringBuilder();

                while (reader.Read())
                {
                    var dropScript = reader.GetString(reader.GetOrdinal("CreateScript"));
                    //var sourceTableName = reader.GetString(reader.GetOrdinal("SourceTableName"));
                    //var targetTableName = reader.GetString(reader.GetOrdinal("TargetTableName"));

                    scriptBuilder.AppendLine(dropScript);
                }

                if (File.Exists(TargetPath))
                {
                    File.Delete(TargetPath);
                }

                File.WriteAllText(TargetPath, scriptBuilder.ToString());

                return DTSExecResult.Success;
            } catch (Exception ex) {
                log.Write(string.Format("{0}.Execute", GetType().FullName), ex.ToString());
                return DTSExecResult.Failure;
            }
        }
    }
}
