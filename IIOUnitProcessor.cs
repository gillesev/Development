using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace E3Retail.TP.HostAuthAdapter.AJB.Comm.FiPay20
{
    public interface IIOUnitProcessor : IDisposable
    {
        Task<string> Send(long msgId, string msg, int timeout);
    }
}
