using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IO
{
    public interface IIOUnitProcessor : IDisposable
    {
        Task<string> Send(long msgId, string msg, int timeout);
    }
}
