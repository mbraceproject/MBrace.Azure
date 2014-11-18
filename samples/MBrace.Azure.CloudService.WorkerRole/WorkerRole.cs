using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Diagnostics;
using Microsoft.WindowsAzure.ServiceRuntime;
using Microsoft.WindowsAzure.Storage;
using Nessos.MBrace.Azure.Runtime;

namespace Nessos.MBrace.Azure.CloudService.WorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        private readonly ManualResetEvent runCompleteEvent = new ManualResetEvent(false);
        private Tasks.RuntimeState _state = null;

        public override void Run()
        {
            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole is running");

            try
            {
                Trace.TraceInformation("Starting MBraze.Azure.Runtime Service");
                Service.StartSync(_state, 10);
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

            bool result = base.OnStart();

            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole has been started");

            Trace.TraceInformation("Activating configuration");
            Service.Configuration = new Config.AzureConfig("", "");

            Trace.TraceInformation("Initializing state");
            _state = Tasks.RuntimeState.InitLocal();

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole is stopping");

            this.runCompleteEvent.WaitOne();

            base.OnStop();

            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole has stopped");
        }
    }
}
