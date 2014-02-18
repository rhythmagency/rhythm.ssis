using System;
using Microsoft.SqlServer.Dts.Runtime;

namespace Rhythm.SSIS.Tasks {
    public static class DTSLoggingExtensions {
        public static void Write(this IDTSLogging log, string eventName, string message)
        {
            if (log == null)
                return;
            byte[] dataBytes = null;
            log.Log(eventName, null, null, null, null, null, message, DateTime.Now, DateTime.Now, 0, ref dataBytes);
        }
    }
}