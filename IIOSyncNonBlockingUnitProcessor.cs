using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IO
{
    /// <summary>
    /// Elementary component able to process a IO message in a synchronous non blocking pattern.
    /// </summary>
    public interface IIOSyncNonBlockingUnitProcessor : IIOUnitProcessor
    {
        /// <summary>
        /// Sends synchronously but non blocking the message and returns a return Code.
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        int Send(string msg);

        /// <summary>
        /// Reads synchronously but non blocking the next message available.
        /// Returns NULL if there is no message to read.
        /// </summary>
        /// <returns></returns>
        string Read();

        /// <summary>
        /// Indicates if there is a message to read.
        /// </summary>
        bool Received { get; }
    }
}
