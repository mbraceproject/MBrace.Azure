using System;
using System.Linq;
using System.Net;
using Microsoft.Azure;
using Microsoft.WindowsAzure.ServiceRuntime;
using MBrace.Azure.Service;

namespace MBrace.Azure.CloudService.WorkerRole
{
    public class WorkerRole : RoleEntryPoint
    {
        private Configuration _config;
        private WorkerService _svc;

        public override void Run()
        {
            _svc.Run();
        }

        public override bool OnStart()
        {
            // Increase disk quota for mbrace filesystem cache.
            string customTempLocalResourcePath = RoleEnvironment.GetLocalResource("LocalMBraceCache").RootPath;
            string storageConnectionString = CloudConfigurationManager.GetSetting("MBrace.StorageConnectionString");
            string serviceBusConnectionString = CloudConfigurationManager.GetSetting("MBrace.ServiceBusConnectionString");

            ServicePointManager.DefaultConnectionLimit = 512;

            bool result = base.OnStart();

            _config = new Configuration(storageConnectionString, serviceBusConnectionString);
            _svc =
                RoleEnvironment.IsEmulated ?
                new WorkerService(_config) : // Avoid long service names when using emulator
                new WorkerService(_config, workerId: RoleEnvironment.CurrentRoleInstance.Id.Split('.').Last());

            _svc.WorkingDirectory = customTempLocalResourcePath;
            _svc.LogFile = "logs.txt";
            _svc.MaxConcurrentWorkItems = Environment.ProcessorCount * 8;

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
                    string storageConnectionString = CloudConfigurationManager.GetSetting("MBrace.StorageConnectionString");
                    string serviceBusConnectionString = CloudConfigurationManager.GetSetting("MBrace.ServiceBusConnectionString");
                    _config = new Configuration(storageConnectionString, serviceBusConnectionString);
                    _svc.Stop();
                    _svc.Configuration = _config;
                    _svc.Start();
                }
            }
        }

        public override void OnStop()
        {
            base.OnStop();
            _svc.Stop();
        }
    }
}
