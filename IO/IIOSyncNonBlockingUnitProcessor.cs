using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IO
{
    public interface IIOSyncNonBlockingUnitProcessor : IDisposable
    {
        int Send(string msg);

        string Read();

        bool Received { get; }
    }
}
