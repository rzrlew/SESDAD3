using System;
using System.IO;
using System.Windows.Forms;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.Remoting;
using System.Runtime.Remoting.Channels;
using System.Runtime.Remoting.Channels.Tcp;
using System.Diagnostics;
using System.Net.Sockets;

namespace SESDAD
{
    public delegate void PuppetMasterFormEventDelegate(string msg);
    
    public class PuppetMaster
    {
        PuppetMasterForm form;
        IDictionary<string, string> processesAddressHash = new Dictionary<string, string>();
        List<SESDADConfig> configList = new List<SESDADConfig>();
        private List<string> toShowMessages = new List<string>();
        private int puppetMasterPort = 9000;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        public static void Main(string[] args)
        {
            if (args.Count() < 2)
            {
                throw new NotImplementedException("Must indicate True or False and puppetmaster address in arguments!");
            }
            Console.WriteLine(bool.FalseString);
            if (bool.Parse(args[0]))
            {
                Application.EnableVisualStyles();
                Application.SetCompatibleTextRenderingDefault(false);
                PuppetMaster puppetMaster = new PuppetMaster();
                Process.Start(TestConstants.puppetSlavePath, args[1]);
                Application.Run(puppetMaster.form);
            }
            else
            {
                Process.Start(TestConstants.puppetSlavePath, args[1]);
            }
        }

        public PuppetMaster()
        {
            form = new PuppetMasterForm();
            form.OnBajorasPrint += new PuppetMasterFormEventDelegate(ShowMessage);
            form.OnSingleCommand = new PuppetMasterFormEventDelegate(RunSingleCommand);
            form.OnScriptCommands = new PuppetMasterFormEventDelegate(RunScript);
            TcpChannel channel = new TcpChannel(puppetMasterPort);
            ChannelServices.RegisterChannel(channel, true);
            RemotePuppetMaster remotePM = new RemotePuppetMaster();
            remotePM.slaveSignIn += new SESDADSlaveConfigurationDelegate(RegisterSlave);
            remotePM.configRequest += new SESDADconfigurationDelegate(searchConfigList);
            remotePM.OnLogMessage += new PuppetMasterLogEventDelegate(ShowLog);
            RemotingServices.Marshal(remotePM, "puppetmaster");
            parseConfigFile(TestConstants.configFilePath);
            PopulateAddressHash();
        }

        private void PopulateAddressHash()
        {
            foreach(SESDADConfig siteConfig in configList)
            {
                foreach(SESDADProcessConfig processConfig in siteConfig.processConfigList)
                {
                    processesAddressHash.Add(processConfig.processName, processConfig.processAddress);
                }
            }
        }
        private string ShowLog(string logMessage)
        {
            ShowMessage(logMessage);
            return "Message Logged!";
        }

        SESDADConfig RegisterSlave()
        {
            foreach(SESDADConfig config in configList)
            {
                if (!config.isDone)
                {
                    config.isDone = true;
                    ShowMessage("Slave for '" + config.siteName + "' is registered!");
                    return config;
                }
            }
            throw new NotImplementedException("No configuration left for slave!");
        }
        private SESDADConfig searchConfigList(string SiteName)  // returns the Config Class of specified Site
        {
            foreach(SESDADConfig c in configList)
            {
                if (c.siteName.Equals(SiteName))
                {
                    return c;
                }
            }
            throw new NotImplementedException();
        }
        public void parseConfigFile(string filename)
        {
            StreamReader file = openConfigFile(filename);
            
            while (true)
            {
                string line = file.ReadLine();
                if (line == null) { break; }    // ends parse if end of file
                string[] args = line.Split(' ');
                switch (args[0])
                {
                    case "Site":
                        SESDADConfig conf = new SESDADConfig(args[1]);
                        conf.parentSiteName = args[3];
                        foreach (SESDADConfig c in configList)
                        {
                            if (c.siteName.Equals(args[3])) // search for Parent Site
                            {
                                c.childrenSiteNames.Add(conf.siteName); // add siteName to parent node child Name List 
                            }
                        }
                        configList.Add(conf);
                        break;

                    case "Process":
                        SESDADProcessConfig ProcessConf = new SESDADProcessConfig();
                        ProcessConf.processName = args[1];
                        ProcessConf.processType = args[3];
                        ProcessConf.processAddress = args[7];
                        switch (args[3])
                        {
                            case "broker":
                                string parentName = null;
                                SESDADConfig config = searchConfigList(args[5]); // search for Configuration Class using Site Name
                                parentName = config.parentSiteName;
                                config.processConfigList.Add(ProcessConf); // adds Process Config to Site Configuration Class 
                                if (parentName != "none") // only needed if not Root Node
                                {
                                    SESDADConfig parentConf = searchConfigList(config.parentSiteName); // Parent Configuration Class
                                    parentConf.childBrokersAddresses.Add(args[7]); // Add process address to parent list
                                    ProcessConf.processParentAddress = parentConf.searchBroker().processAddress; // address of 'first' broker of Site
                                }
                                break;

                            case "publisher":
                            case "subscriber":
                                ProcessConf.processParentAddress = searchConfigList(args[5]).searchBroker().processAddress;
                                searchConfigList(args[5]).processConfigList.Add(ProcessConf);
                                break;

                        }
                        break;
                }
            }

        }
        public void RunSingleCommand(string command)
        {
            string[] commandTokens = command.Split(' ');
            SESDADRemoteProcessControlInterface remoteProcess;
            switch (commandTokens[0])
            {               
                case "Status":
                    foreach (KeyValuePair<string, string> entry in processesAddressHash)
                    {
                        remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), entry.Value);
                        ShowMessage(remoteProcess.Status());
                    }
                    break;
                case "Freeze":
                    ShowMessage("Freezing process '" + commandTokens[1] + "' !");
                    remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), processesAddressHash[commandTokens[1]]);
                    remoteProcess.Freeze();
                    break;
                case "Unfreeze":
                    ShowMessage("Unfreezing process '" + commandTokens[1] + "' !");
                    remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), processesAddressHash[commandTokens[1]]);
                    remoteProcess.Unfreeze();
                    break;
                case "Crash":
                    ShowMessage("Crashing process '" + commandTokens[1] + "' !");
                    remoteProcess = (SESDADRemoteProcessControlInterface)Activator.GetObject(typeof(SESDADRemoteProcessControlInterface), processesAddressHash[commandTokens[1]]);
                    try {
                        remoteProcess.Crash();
                    }
                    catch (IOException)
                    {
                        ShowMessage("Process '" + commandTokens[1] + "' has crashed!");
                    }
                    break;
                case "Publisher":
                    ShowMessage("Asking publisher '" + commandTokens[1] + "' to publish on topic '" + commandTokens[3]  + "' !");
                    RemotePublisher remotePublisher = (RemotePublisher)Activator.GetObject(typeof(RemotePublisher), processesAddressHash[commandTokens[1]]);
                    remotePublisher.Publish(commandTokens[3], commandTokens[4]);
                    break;
            }
        }

        private void RunScript(string script)
        {
            string[] lines = script.Split(Environment.NewLine.ToCharArray());
            foreach(string line in lines)
            {
                RunSingleCommand(line);
            }
        }

        private StreamReader openConfigFile(string fileName)
        {
            return new StreamReader(File.Open(fileName, FileMode.Open));
        }
        void ShowMessage(string msg)    // print given string in puppet master form
        {
            object[] arguments = new object[1];
            arguments[0] = "[" + DateTime.Now.Hour + ":" + DateTime.Now.Minute + ":" + DateTime.Now.Second + "] " + msg + Environment.NewLine;
            form.Invoke(new PuppetMasterFormEventDelegate(form.appendToOutputWindow), arguments);
        }
    }
}
