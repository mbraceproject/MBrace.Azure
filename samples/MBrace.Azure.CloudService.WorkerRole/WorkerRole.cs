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
        private RuntimeState _state;
        private System.Threading.Tasks.Task _svc;

        public override void Run()
        {
            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole is running");

            Trace.TraceInformation("Starting MBraze.Azure.Runtime Service");
            _svc = Service.StartAsTask(_state, msg => Trace.WriteLine(msg, "MBrace.Runtime.Worker"), 10);
            _svc.Wait();
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            bool result = base.OnStart();

            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole has been started");

            Trace.TraceInformation("Activating configuration");
            Service.Configuration = new AzureConfig("", "");

            Trace.TraceInformation("Initializing state");
            _state = RuntimeState.InitLocal();

            return result;
        }

        public override void OnStop()
        {
            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole is stopping");

            base.OnStop();

            Trace.TraceInformation("MBrace.Azure.CloudService.WorkerRole has stopped");
        }
    }
}
