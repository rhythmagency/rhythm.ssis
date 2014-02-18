using System;
using System.ComponentModel;
using System.IO;
using Microsoft.SqlServer.Dts.Runtime;
using Renci.SshNet;

namespace Rhythm.SSIS.Tasks
{
    [DtsTask(
        DisplayName = "SFTP Download",
        Description = "Download a file via SFTP")]
    public class SftpDownloadTask : Task
    {
        public SftpDownloadTask() {
            Port = 22;
        }

        [Category("SFTP Download")]
        public string Host { get; set; }

        [Category("SFTP Download")]
        public int Port { get; set; }

        [Category("SFTP Download")]
        public string Username { get; set; }

        [Category("SFTP Download")]
        public string Password { get; set; }

        [Category("SFTP Download")]
        public string SourcePath { get; set; }

        [Category("SFTP Download")]
        public string TargetPath { get; set; }

        public override DTSExecResult Validate(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log) {
            return (
                !string.IsNullOrWhiteSpace(Host)
                && Port > 0
                && !string.IsNullOrWhiteSpace(Username)
                && !string.IsNullOrWhiteSpace(Password)
                && !string.IsNullOrWhiteSpace(SourcePath)
                && !string.IsNullOrWhiteSpace(TargetPath)
                ) ? DTSExecResult.Success : DTSExecResult.Failure;
        }

        public override DTSExecResult Execute(Connections connections, VariableDispenser variableDispenser, IDTSComponentEvents componentEvents, IDTSLogging log, object transaction) {

            try {
                using (var sftp = new SftpClient(Host, Port, Username, Password))
                {
                    sftp.Connect();

                    using (var file = File.OpenWrite(TargetPath))
                    {
                        sftp.DownloadFile(SourcePath, file);
                    }

                    return DTSExecResult.Success;
                }   
            } catch (Exception ex) {
                log.Write(string.Format("{0}.Execute", GetType().FullName), ex.ToString());
                return DTSExecResult.Failure;
            }
        }
    }
}
