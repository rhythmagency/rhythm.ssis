using System;
using System.ComponentModel;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using Microsoft.SqlServer.Dts.Runtime;

namespace Rhythm.SSIS.Tasks {
    [DtsTask(
        DisplayName = "Write Drop Keys Script",
        Description = "Writes a SQL script to the specified file that drops foriegn keys for the given connection.")]
    public class WriteDropKeysScriptTask : Task {
        private const string SELECT_STATEMENT_PLACEHOLDER = "{SELECT_STATEMENT_PLACEHOLDER}";

        [Category("Write Drop Keys Script")]
        public string ConnectionName { get; set; }

        [Category("Write Drop Keys Script")]
        public string TargetPath { get; set; }

        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log) {
            return (
                connections.Contains(ConnectionName)
                ) ? DTSExecResult.Success : DTSExecResult.Failure;
        }

        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction) {

            try {
                var connectionManager = connections[ConnectionName];
                var connection = connectionManager.AcquireConnection(transaction) as SqlConnection;

                var dropKeysScript = Resources.DropCreateFKScriptCommon.Replace(SELECT_STATEMENT_PLACEHOLDER, Resources.DropFKScript);

                var command = new SqlCommand(dropKeysScript, connection);

                var reader = command.ExecuteReader();

                var scriptBuilder = new StringBuilder();

                while (reader.Read())
                {
                    var dropScript = reader.GetString(reader.GetOrdinal("DropScript"));
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