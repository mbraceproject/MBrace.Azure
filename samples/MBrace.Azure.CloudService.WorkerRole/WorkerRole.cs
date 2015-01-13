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
using MBrace.Azure.Runtime;
using MBrace.Azure.Runtime.Common;
using MBrace.Azure.Store;
using MBrace.Store;

namespace MBrace.Azure.CloudService.WorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        private Service _svc;

        public override void Run()
        {
            _svc.Start();
        }

        public override bool OnStart()
        {
            // Set the maximum number of concurrent connections
            ServicePointManager.DefaultConnectionLimit = 12;

            bool result = base.OnStart();

            var config = Configuration.Default
                            .WithStorageConnectionString(CloudConfigurationManager.GetSetting("MBrace.ServiceBusConnectionString"))
                            .WithServiceBusConnectionString(CloudConfigurationManager.GetSetting("MBrace.StorageConnectionString"));

            _svc = new Service(config, serviceId : RoleEnvironment.CurrentRoleInstance.Id);
            _svc.AttachLogger(new CustomLogger(s => Trace.WriteLine(s)));

            RoleEnvironment.Changed += RoleEnvironment_Changed;

            return result;
        }

        void RoleEnvironment_Changed(object sender, RoleEnvironmentChangedEventArgs e)
        {
            foreach (var item in e.Changes.OfType<RoleEnvironmentTopologyChange>())
            {
                if (item.RoleName == RoleEnvironment.CurrentRoleInstance.Role.Name)
                { 
                    // take any action needed on instance count modification; gracefully shrink etc
                }
            }

            foreach (var item in e.Changes.OfType<RoleEnvironmentConfigurationSettingChange>())
            {
                if (item.ConfigurationSettingName == "MBrace.ServiceBusConnectionString"
                    || item.ConfigurationSettingName == "MBrace.StorageConnectionString")
                {
                    // alter the service configuration
                }
            }
        }

        public override void OnStop()
        {
            base.OnStop();
        }
    }
}
