using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SESDAD
{
    class PuppetSlave
    {

        PuppetMasterRemote remotePuppetMaster;
        string siteName;

        public static void Main(string[] args)
        {

        }

        public PuppetSlave(string puppetMasterAddress, string siteName)
        {
            this.siteName = siteName;
            remotePuppetMaster = (PuppetMasterRemote)Activator.GetObject(typeof(PuppetMasterRemote), puppetMasterAddress);
            remotePuppetMaster.GetConfiguration(siteName);
        }
    }
}
