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

namespace SESDAD
{
    public delegate void PuppetMasterFormEvent(string msg);
    
    public class PuppetMaster
    {
        PuppetMasterForm form;
        IDictionary<string, string> brokerHash = new Dictionary<string, string>();
        List<SESDADConfig> configList = new List<SESDADConfig>();
        private List<string> toShowMessages = new List<string>();
        private int puppetMasterPort = 9000;
        private int slavePortCounter = 5000;

        /// <summary>
        /// The main entry point for the application.
        /// </summary>
        [STAThread]
        public static void Main()
        {
            Application.EnableVisualStyles();
            Application.SetCompatibleTextRenderingDefault(false);
            PuppetMaster puppetMaster = new PuppetMaster();
            puppetMaster.BootStrapSystem();
            Application.Run(puppetMaster.form);
        }

        public PuppetMaster()
        {
            form = new PuppetMasterForm();
            form.OnBajorasPrint += new PuppetMasterFormEvent(ShowMessage);
            TcpChannel channel = new TcpChannel(puppetMasterPort);
            ChannelServices.RegisterChannel(channel, true);
            RemotePuppetMaster remotePM = new RemotePuppetMaster();
            remotePM.brokerSignIn += new PuppetMasterEvent(RegisterBroker);
            remotePM.configRequest += new SESDADconfiguration(searchConfigList);
            RemotingServices.Marshal(remotePM, "puppetmaster");
            parseConfigFile(TestConstants.configFilePath);
        }

        private void BootStrapSystem()  // launches a PuppetSlave process for each Configuration Class created after reading config file
        {
            foreach(SESDADConfig slaveConfig in configList) 
            {
                int nextSlavePort = ++slavePortCounter;
                toShowMessages.Add("Slave for " + slaveConfig.SiteName + " is on port " + nextSlavePort);
                Process.Start(TestConstants.puppetSlavePath, "tcp://localhost:" + puppetMasterPort + "/puppetmaster " + slaveConfig.SiteName + " " +  nextSlavePort);
            }
        }

        private SESDADConfig searchConfigList(string SiteName)  // returns the Config Class of specified Site
        {
            foreach(SESDADConfig c in configList)
            {
                if (c.SiteName.Equals(SiteName))
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
                        {
                            SESDADConfig conf = new SESDADConfig(args[1]);
                            conf.ParentSiteName = args[3];
                            foreach (SESDADConfig c in configList)
                            {
                                if (c.SiteName.Equals(args[3])) // search for Parent Site
                                {
                                    c.ChildrenSiteNames.Add(conf.SiteName); // add siteName to parent node child Name List 
                                }
                            }
                            configList.Add(conf);
                            break;
                        }

                    case "Process":
                        {
                            SESDADProcessConfig ProcessConf = new SESDADProcessConfig();
                            ProcessConf.ProcessName = args[1];
                            ProcessConf.ProcessType = args[3];
                            ProcessConf.ProcessAddress = args[7];
                            switch (args[3])
                            {
                                case "broker":
                                    {
                                        string parentName = null;                                   
                                        
                                        SESDADConfig conf = searchConfigList(args[5]); // search for Configuration Class using Site Name
                                        parentName = conf.ParentSiteName;
                                        conf.ProcessConfigList.Add(ProcessConf); // adds Process Config to Site Configuration Class 

                                        if (parentName != "none") // only needed if not Root Node
                                        {
                                            SESDADConfig parentConf = searchConfigList(conf.ParentSiteName); // Parent Configuration Class
                                            parentConf.ChildBrokersAddresses.Add(args[7]); // Add process address to parent list
                                            ProcessConf.ProcessParentAddress = parentConf.searchBroker().ProcessAddress; // address of 'first' broker of Site
                                        }
                                        break;
                                    }
                                case "subscriber":
                                    {                                       
                                        ProcessConf.ProcessParentAddress = searchConfigList(args[5]).searchBroker().ProcessAddress;
                                        searchConfigList(args[5]).ProcessConfigList.Add(ProcessConf);
                                        break;
                                    }
                            }                           
                            break;
                        }
                }
            }
        }

        private StreamReader openConfigFile(string fileName)
        {
            return new StreamReader(File.Open(fileName, FileMode.Open));
        }

        string RegisterBroker(PuppetMasterEventArgs args)   // saves broker address in Hash
        {
            ShowMessage("Broker \"" + args.BrokerName + "\" at \"" + args.Address + "\" signing in!");
            string brokerName = args.SiteName;
            brokerHash.Add(brokerName , args.Address);
            return brokerName;
        }

        void ShowMessage(string msg)    // print given string in puppet master form
        {
            string prependMessages = "";
            if(toShowMessages.Count > 0)
            {
                foreach(string m in toShowMessages)
                {
                    prependMessages += m + Environment.NewLine;
                }
            }
            object[] arguments = new object[1];
            arguments[0] = prependMessages + msg + Environment.NewLine;
            form.Invoke(new PuppetMasterFormEvent(form.appendToOutputWindow), arguments);
        }
    }
}
