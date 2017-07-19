using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MOFSyncJob
{
    class Program
    {
        static void Main(string[] args)
        {
            //MOFDataSync.Start(true);
            MOFDataSync.Start(false);
            MOFDataSync.EDFIData();
        }
    }
}
