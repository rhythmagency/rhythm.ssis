using System;
using System.ComponentModel;
using System.IO;
using Microsoft.SqlServer.Dts.Runtime;
using Renci.SshNet;

namespace Rhythm.SSIS.Tasks {
    [DtsTask(
        DisplayName = "SFTP Upload",
        Description = "Upload a file via SFTP")]
    public class SftpUploadTask : Task
    {
        public SftpUploadTask()
        {
            Port = 22;
        }

        [Category("SFTP Upload")]
        public string Host { get; set; }

        [Category("SFTP Upload")]
        public int Port { get; set; }

        [Category("SFTP Upload")]
        public string Username { get; set; }

        [Category("SFTP Upload")]
        public string Password { get; set; }

        [Category("SFTP Upload")]
        public string SourcePath { get; set; }

        [Category("SFTP Upload")]
        public string TargetPath { get; set; }

        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log)
        {
            return (
                !string.IsNullOrWhiteSpace(Host)
                && Port > 0
                && !string.IsNullOrWhiteSpace(Username)
                && !string.IsNullOrWhiteSpace(Password)
                && !string.IsNullOrWhiteSpace(SourcePath)
                && !string.IsNullOrWhiteSpace(TargetPath)
                ) ? DTSExecResult.Success : DTSExecResult.Failure;
        }

        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction)
        {

            try
            {
                using (var sftp = new SftpClient(Host, Port, Username, Password))
                {
                    sftp.Connect();

                    using (var file = File.OpenRead(SourcePath))
                    {
                        sftp.UploadFile(file, TargetPath);
                    }

                    return DTSExecResult.Success;
                }
            }
            catch (Exception ex)
            {
                log.Write(string.Format("{0}.Execute", GetType().FullName), ex.ToString());
                return DTSExecResult.Failure;
            }
        }
    }
}