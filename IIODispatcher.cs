using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IO
{
    /// <summary>
    /// Dispatches a message to an underlying IO processor infrastructure
    /// (it could be 1 or multiple processors).
    /// </summary>
    public interface IIODispatcher : IDisposable
    {
        Task<string> Send(string msg, int timeout);
    }
}
