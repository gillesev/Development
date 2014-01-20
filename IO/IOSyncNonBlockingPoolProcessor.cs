using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using AJB.Lib;
using IO;
using IssRetail.CoreSvc;

namespace IO
{
    public struct Message
    {
        public string Body { get; set; }
        public long Id { get; set; }
        public int Timeout { get; set; }
    }

    public enum TaskStateStatus
    {
        Undefined = 0,
        ToProcess = 1,
        BeingProcessed = 2,
        Processed = 3,
        Error = 4,
        MarkedForDeletion = 5,
    }

    public class TaskState : IAsyncResult
    {
        public TaskState(long taskId, string requestMsg, int timeout, AsyncCallback callback)
        {
            _taskId = taskId;
            _requestMsg = requestMsg;
            _callback = callback;
            Status = (int)TaskStateStatus.ToProcess;
            _CreationDateTime = DateTime.Now;
            _timeout = timeout;
        }

        #region Private Members

        private string _requestMsg;
        private long _taskId;
        private AsyncCallback _callback;
        private DateTime _CreationDateTime;
        private int _timeout;

        public DateTime QueuedUp { get; set; }
        public DateTime Sent { get; set; }
        public DateTime Received { get; set; }
        public DateTime MarkedForDeletion { get; set; }
        public int Timeout { get { return _timeout; } }

        #endregion

        #region Public Members

        public long TaskId { get { return _taskId; } }

        public int Status { get; set; }

        public string Response { get; set; }

        public AsyncCallback CallBack { get { return _callback; } }

        public DateTime CreationDateTime { get { return _CreationDateTime; } }

        #endregion

        #region IAsyncResult Members

        public object AsyncState
        {
            get { return _requestMsg; }
        }

        public System.Threading.WaitHandle AsyncWaitHandle
        {
            get { return null; }
        }

        public bool CompletedSynchronously
        {
            get { return false; }
        }

        public bool IsCompleted
        {
            get { return Status == (int)TaskStateStatus.Processed || Status == (int)TaskStateStatus.Error; }
        }

        #endregion
    }

    /// <summary>
    /// Note: it is possible for the Send thread to miss a signal that new IO work has been queued up as it may be waiting on a cleanup job to be completed.
    /// in which case the cleanup job will eventually complete, the Send thread will be resumed and not wait for more than (default) 30' to wake up and finally process the IO task.
    /// Note: the issue can be aggravated by the fact that even though the Send thread is ready to be suspended, the Receive thread may not and may be processing reads which delays the start of the cleanup job...
    /// </summary>
    public class IOUnitProcessor : IIOSyncNonBlockingUnitProcessor
    {
        private const string CLASS_NAME = "IOUnitProcessor";

        /// <summary>
        /// Synchronous Connect using the connection timeout.
        /// </summary>
        /// <returns></returns>
        public int Connect()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Initialize", "Processor {0}", this.Index))
            {
                if (_connection.Connect() == true)
                {
                    var timeoutDT = DateTime.Now.Add(new TimeSpan(0, 0, 0, 0, _connectionTimeout));

                    while (string.IsNullOrEmpty(_connection.Current_ISP_Connected) && DateTime.Now < timeoutDT)
                    {
                    }

                    return !string.IsNullOrEmpty(_connection.Current_ISP_Connected) ? 0 : -1;
                }

                return -1;
            }
        }

        /// <summary>
        /// Synchronous Disconnect
        /// </summary>
        /// <returns></returns>
        public int Disconnect()
        {
            return 0;
        }

        /// <summary>
        /// Synchronous Send
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public int Send(string msg)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Send", "Message {0} on Processor {1}", msg, this.Index))
            {
                return _connection.Send(msg) == true ? 0 : -1;
            }
        }

        /// <summary>
        /// Synchronous Read
        /// </summary>
        /// <returns></returns>
        public string Read()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Read", "Processor {0}", this.Index))
            {
                return _connection.Read();
            }
        }

        /// <summary>
        /// True if Data has been received
        /// </summary>
        public bool Received 
        {
            get 
            {
                using (var trace = Tracer.Create(CLASS_NAME, "Received", "Processor {0}", this.Index))
                {
                    return _connection.DataAvailable > 0 ? true : false;
                }
            } 
        }

        /// <summary>
        /// Unique identifier for the processor. Used to correlate a queue and its processor.
        /// </summary>
        public int Index
        {
            get { return _index; }
        }

        /// <summary>
        /// Signals the Send thread for new IO task
        /// </summary>
        public void SignalSendIO()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SignalSendIO", "Processor {0}", this.Index))
            {
                _mreSend.Set();
                _lastSignaledDateTime = DateTime.Now;
            }
        }

        /// <summary>
        /// Suspends the Send thread until new IO task is queued up.
        /// </summary>
        /// <returns></returns>
        public bool SuspendSendWaitingOnIO()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SuspendSendWaitingOnIO", "Processor {0}", this.Index))
            {
                if (!_lastSignaledDateTime.HasValue || _lastSignaledDateTime < DateTime.Now.Subtract(new TimeSpan(0, 0, 0, 0, _minimumPeriodBeforeWaitOnSend)))
                {
                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Waiting to be signaled for Send on Processor {0}", this.Index);
                    _mreSend.WaitOne(_waitSendTimeout);
                    return true;
                }

                return false;
            }
        }

        /// <summary>
        /// Signals the Receive thread for new IO task
        /// </summary>
        public void SignalReceiveIO()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SignalReceiveIO", "Processor {0}", this.Index))
            {
                _mreReceive.Set();
            }
        }

        /// <summary>
        /// Suspend Receive thread until new IO task is queued up.
        /// </summary>
        /// <returns></returns>
        public bool SuspendReceiveWaitingOnIO()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SuspendReceiveWaitingOnIO", "Processor {0}", this.Index))
            {
                if (!_lastSignaledDateTime.HasValue || _lastSignaledDateTime < DateTime.Now.Subtract(new TimeSpan(0, 0, 0, 0, _minimumPeriodBeforeWaitOnReceive)))
                {
                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Waiting to be signaled for Receive on Processor {0}", this.Index);
                    return _mreReceive.WaitOne(_waitReceiveTimeout);
                }
                Thread.Sleep(50);
                return false;
            }
        }

        /// <summary>
        /// Signals an intent to perform cleanup (called by cleanup timer thread)
        /// </summary>
        /// <param name="doCleanUp"></param>
        public void SignalIntentToCleanUp(bool doCleanUp)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SignalIntentToCleanUp", "Processor {0}", this.Index))
            {
                Interlocked.Exchange(ref _intentToCleanUp, (doCleanUp) ? 1 : 0);
            }
        }

        /// <summary>
        /// Returns TRUE if an intent to perform some cleanup has been set by the cleanup timer thread.
        /// </summary>
        public bool IntentToCleanUpSignaled
        {
            get { return _intentToCleanUp == 1; }
        }

        /// <summary>
        /// Suspend the cleanup thread until signaled by both the Send and Receive threads
        /// </summary>
        /// <returns></returns>
        public bool WaitToCleanUp()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "WaitToCleanUp", "Processor {0}", this.Index))
            {
                return WaitHandle.WaitAll(new WaitHandle[2] { _mreSendAckIntentToCleanup, _mreReceiveAckIntentToCleanup });
            }
        }

        /// <summary>
        /// Signals the cleanup thread that the Send thread has acknowledge the intent to cleanup
        /// </summary>
        public void SignalCleanUpWithSendAck()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SignalCleanUpWithSendAck", "Processor {0}", this.Index))
            {
                _mreSendAckIntentToCleanup.Set();
            }
        }

        /// <summary>
        /// Signals the cleanup thread that the Receive thread has acknowledge the intent to cleanup
        /// </summary>
        public void SignalCleanUpWithReceiveAck()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SignalCleanUpWithReceiveAck", "Processor {0}", this.Index))
            {
                _mreReceiveAckIntentToCleanup.Set();
            }
        }

        /// <summary>
        /// Signals the Send thread that the cleanup is complete
        /// </summary>
        public void SignalSendWithCleanUpCompleteAck()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SignalSendWithCleanUpCompleteAck", "Processor {0}", this.Index))
            {
                _mreSignalSendAckCleanUpComplete.Set();
            }
        }

        /// <summary>
        /// Signals the Receive thread that the cleanup is complete
        /// </summary>
        public void SignalReceiveWithCleanUpCompleteAck()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SignalReceiveWithCleanUpCompleteAck", "Processor {0}", this.Index))
            {
                _mreSignalReceiveAckCleanUpComplete.Set();
            }
        }

        /// <summary>
        /// Suspend forever the Send thread until the cleanup thread signals and frees it.
        /// </summary>
        public void SuspendSendWaitingOnCleanUp()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SuspendSendWaitingOnCleanUp", "Processor {0}", this.Index))
            {
                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Send thread waiting to be signaled by Cleanup thread on Processor {0}", this.Index);
                _mreSignalSendAckCleanUpComplete.WaitOne();
            }
        }

        /// <summary>
        /// Suspend forever the Send thread until the cleanup thread signals and frees it.
        /// </summary>
        public void SuspendReceiveWaitingOnCleanUp()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SuspendReceiveWaitingOnCleanUp", "Processor {0}", this.Index))
            {
                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Receive thread waiting to be signaled by Cleanup thread on Processor {0}", this.Index);
                _mreSignalReceiveAckCleanUpComplete.WaitOne();
            }
        }

        #region Private Members

        private XXX _connection = null;
        private int _index = 0;
        private AutoResetEvent _mreSend;
        private DateTime? _lastSignaledDateTime = null;
        private AutoResetEvent _mreReceive;
        private int _connectionTimeout;
        private int _waitSendTimeout;
        private int _waitReceiveTimeout;
        private int _intentToCleanUp;
        private AutoResetEvent _mreSendAckIntentToCleanup;
        private AutoResetEvent _mreReceiveAckIntentToCleanup;
        private int _minimumPeriodBeforeWaitOnSend;
        private int _minimumPeriodBeforeWaitOnReceive;
        private AutoResetEvent _mreSignalSendAckCleanUpComplete;
        private AutoResetEvent _mreSignalReceiveAckCleanUpComplete;

        #endregion

        #region Ctor

        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="index">Used to correlate a queue with its processor</param>
        /// <param name="connectionTimeout">specific to the backend component used to process the task</param>
        /// <param name="waitSendTimeout">Maximum amount of time the Send thread will be waiting for IO</param>
        /// <param name="waitReceiveTimeout">Maximum amount of time the Receive thread will be waiting for read IO</param>
        /// <param name="minimumPeriodBeforeWaitOnSend">Minimum amount of time that needs to elapse between now and the last time the Send thread was signaled for IO before suspending the Send thread</param>
        /// <param name="minimumPeriodBeforeWaitOnReceive">Minimum amount of time that needs to elapse between now and the last time the Receive thread was signaled for IO before suspending the Receive thread</param>
        public IOUnitProcessor(int index, int connectionTimeout, int waitSendTimeout, int waitReceiveTimeout, int minimumPeriodBeforeWaitOnSend, int minimumPeriodBeforeWaitOnReceive)
        {
            _index = index;
            _connection = new XXX(24900, "127.0.0.1", false);
            _connectionTimeout = connectionTimeout;
            _connection.ConnectTimeout = connectionTimeout;
            _mreSend = new AutoResetEvent(false);
            _mreReceive = new AutoResetEvent(false);
            _mreSendAckIntentToCleanup = new AutoResetEvent(false);
            _mreReceiveAckIntentToCleanup = new AutoResetEvent(false);
            _waitSendTimeout = waitSendTimeout;
            _waitReceiveTimeout = waitReceiveTimeout;
            _minimumPeriodBeforeWaitOnSend = minimumPeriodBeforeWaitOnSend;
            _minimumPeriodBeforeWaitOnReceive = minimumPeriodBeforeWaitOnReceive;
            _mreSignalSendAckCleanUpComplete = new AutoResetEvent(false);
            _mreSignalReceiveAckCleanUpComplete = new AutoResetEvent(false);
        }

        #endregion

        public void Dispose()
        {
            
        }
    }

    public class IOSyncNonBlockingPoolProcessor : IIOProcessor
    {
        private const string CLASS_NAME = "IOSyncNonBlockingPoolProcessor";

        #region Private Members

        /// <summary>
        /// Nb of processors to spawn
        /// </summary>
        private int _processorCount = 1;

        private int _connectionTimeout = 2000;
        private int _waitSendTimeout = 30000;
        private int _waitReceiveTimeout = 30000;
        private int _cleanUpTimerDueTime = 30000;
        private int _cleanUpTimerInterval = 30000;
        private int _minimumPeriodBeforeWaitOnSend = 10000;
        private int _minimumPeriodBeforeWaitOnReceive = 10000;

        /// <summary>
        /// Minium # of taks marked for deletion before any cleanup job kicks in
        /// </summary>
        private int _markedForDeletionMinimum = 100;

        /// <summary>
        /// collection of processors. A queue and its processor are associated by their index.
        /// </summary>
        private List<IOUnitProcessor> _processors;

        /// <summary>
        /// collection of dictionaries of task state objects. A queue and its processor are associated by their index.
        /// </summary>
        private Dictionary<int, Dictionary<long, TaskState>> _queues;
        
        /// <summary>
        /// collection of timers. A timer and its processor are associated by their index.
        /// </summary>
        private Dictionary<int, Timer> _timers;

        /// <summary>
        /// Incremental message Id used to identify a task state with the response from the Read call.
        /// </summary>
        private long _nextMsgId = 0;

        /// <summary>
        /// TRUE if resources have been disposed of.
        /// </summary>
        private bool _disposed;

        #endregion

        #region Ctors

        /// <summary>
        /// Ctor. All parameters can be externalized in a config file.
        /// </summary>
        public IOSyncNonBlockingPoolProcessor(int processorCount, int connectionTimeout, int waitSendTimeout, int waitReceiveTimeout, int minimumPeriodBeforeWaitOnSend, int minimumPeriodBeforeWaitOnReceive, int cleanUpTimerDueTime, int cleanUpTimerInterval, int minimumMarkedForDeletionBeforeGCed)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Ctor"))
            {
                this._processorCount = processorCount;
                this._connectionTimeout = connectionTimeout;
                this._waitSendTimeout = waitSendTimeout;
                this._waitReceiveTimeout = waitReceiveTimeout;
                this._cleanUpTimerDueTime = cleanUpTimerDueTime;
                this._cleanUpTimerInterval = cleanUpTimerInterval;
                this._minimumPeriodBeforeWaitOnSend = minimumPeriodBeforeWaitOnSend;
                this._minimumPeriodBeforeWaitOnReceive = minimumPeriodBeforeWaitOnReceive;
                this._markedForDeletionMinimum = minimumMarkedForDeletionBeforeGCed;
            }
        }

        #endregion

        #region Private Methods

        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_processors != null)
                    {
                        for (int k = 0; k < _processors.Count(); k++)
                        {
                            _processors[k].Dispose();
                        }
                    }

                    if (_timers != null)
                    {
                        for (int k = 0; k < _timers.Count(); k++)
                        {
                            _timers[k].Dispose();
                        }
                    }
                }

                _queues = null;
                _timers = null;
                _processors = null;

                _disposed = true;
            }
        }

        /// <summary>
        /// Executes a cleanup cycle.
        /// </summary>
        /// <param name="p"></param>
        private void RunCleanUp(object p)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "RunCleanUp"))
            {
                var processor = p as IOUnitProcessor;

                // stop the timer
                _timers[processor.Index].Change(Timeout.Infinite, _cleanUpTimerInterval);

                if (_queues[processor.Index].Count(ts => ts.Value.Status == (int)TaskStateStatus.MarkedForDeletion) > _markedForDeletionMinimum)
                {

                    // signal intent to clean-up
                    processor.SignalIntentToCleanUp(true);

                    // wait signal from both the Send/Receive threads to start
                    processor.WaitToCleanUp();

                    var dic = _queues[processor.Index];

                    var copy = dic.ToList();

                    var en = copy.GetEnumerator();

                    while (en.MoveNext())
                    {
                        dic.Remove(en.Current.Value.TaskId);
                    }
                }

                _timers[processor.Index].Change(_cleanUpTimerInterval, _cleanUpTimerInterval);

                // signal both threads they can resume...
                processor.SignalIntentToCleanUp(false);
                processor.SignalSendWithCleanUpCompleteAck();
                processor.SignalReceiveWithCleanUpCompleteAck();
            }
        }

        /// <summary>
        /// Executes the infinite loop run on the Send dedicated thread pool thread.
        /// </summary>
        /// <param name="p"></param>
        private void RunSend(object p)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "RunSend"))
            {
                List<TaskState> listToProcess = null;

                var processor = p as IOUnitProcessor;

                // if nb of tasks in ToProcess status, wait on mre
                // when a task is queued up, signal mre...
                // when done with processing the set of tasks in ToProcess status, reset mre...
                while (true)
                {
                    try
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "RunSend loop started on Processor {0}", processor.Index);

                        if (processor.IntentToCleanUpSignaled)
                        {
                            processor.SignalCleanUpWithSendAck();

                            processor.SuspendSendWaitingOnCleanUp();
                        }

                        // by this time the IntentToCleanUpSignaled should have been reset to FALSE.
                        listToProcess = null;
                        lock (_queues[processor.Index])
                        {
                            try
                            {
                                listToProcess = _queues[processor.Index].Select(kvp => kvp.Value).Where(task => task.Status == (int)TaskStateStatus.ToProcess).ToList();
                            }
                            catch (Exception e)
                            {
                            }

                            if (listToProcess != null)
                            {
                                foreach (var task in listToProcess)
                                {
                                    task.Status = (int)TaskStateStatus.BeingProcessed;
                                }
                            }
                        }

                        if (listToProcess != null && listToProcess.Count > 0)
                        {
                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "RunSend loop found {0} candidates ToProcess on Processor {1}", listToProcess.Count, processor.Index);
                            
                            foreach (var task in listToProcess)
                            {
                                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Processing RunSend task Id {0} on Processor {1}", task.TaskId, processor.Index);

                                if (DateTime.Now <= task.CreationDateTime.Add(new TimeSpan(0, 0, 0, 0, task.Timeout)))
                                {
                                    if (processor.Connect() == 0)
                                    {
                                        if (processor.Send(task.AsyncState.ToString()) != 0)
                                        {
                                            // TODO: error in sending
                                            // need to return error to caller.
                                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "FAILED sending task Id {0} on Processor {1}", task.TaskId, processor.Index);
                                            task.Status = (int)TaskStateStatus.Error;
                                        }
                                        else
                                        {
                                            task.Sent = DateTime.Now;
                                        }
                                    }
                                    else
                                    {
                                        // TODO: do re-connect...
                                        // after X attempts to re-connect, release task and set status to ToProcess.
                                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "FAILED sending task Id {0} on Processor {1} which could not be intialized", task.TaskId, processor.Index);
                                        task.Status = (int)TaskStateStatus.ToProcess;
                                    }
                                }
                                else
                                {
                                    task.Status = (int)TaskStateStatus.Error;

                                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "TIMED OUT sending task Id {0} on Processor {1}", task.TaskId, processor.Index);
                                }
                            }
                        }
                        else
                        {
                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "RunSend loop found NO candidates ToProcess on Processor {0}", processor.Index);

                            if (!processor.SuspendSendWaitingOnIO())
                            {
                                Thread.Sleep(50);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "An error occurred while processing RunSend loop on Processor {0}. Exception is {1}", processor.Index, e);
                    }
                }
            }
        }

        /// <summary>
        /// Executes the infinite loop run on the Receive dedicated thread pool thread.
        /// </summary>
        /// <param name="p"></param>
        private void RunReceive(object p)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "RunReceive"))
            {
                var processor = p as IOUnitProcessor;

                while (true)
                {
                    try
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "RunReceive loop started on Processor {0}", processor.Index);

                        if (processor.IntentToCleanUpSignaled)
                        {
                            processor.SignalCleanUpWithReceiveAck();

                            processor.SuspendReceiveWaitingOnCleanUp();
                        }

                        TaskState t = null;
                        long msgId = 0;
                        string resp = null;

                        // poll the processor to check if it did receive a response
                        if (processor.Received)
                        {
                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Processor {0} received data", processor.Index);

                            resp = processor.Read();

                            Debug.Print(resp);

                            msgId = GetMsgId(resp);

                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Read message Id {0}. Message is {1} on Processor {2}", msgId, resp, processor.Index);

                            if (msgId >= 0)
                            {
                                if (_queues[processor.Index].ContainsKey(msgId))
                                {
                                    t = _queues[processor.Index][msgId];

                                    if (t != null)
                                    {
                                        if (t.Status == (int)TaskStateStatus.BeingProcessed && DateTime.Now <= t.CreationDateTime.Add(new TimeSpan(0, 0, 0, 0, t.Timeout)))
                                        {
                                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Processing receiving of Task Id {0} on Processor {1}", msgId, processor.Index);

                                            lock (_queues[processor.Index])
                                            {
                                                if (t.Status == (int)TaskStateStatus.BeingProcessed)
                                                {
                                                    t.Status = (int)TaskStateStatus.Processed;
                                                    t.Received = DateTime.Now;
                                                }
                                                else
                                                {
                                                    t = null;
                                                }
                                            }

                                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Processing receiving of Task Id {0} on Processor {1}", msgId, processor.Index);

                                            if (t != null)
                                            {
                                                t.Response = resp;
                                                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Invoking call back for Task Id {0} on Processor {1}", msgId, processor.Index);
                                                t.CallBack.Invoke(t);
                                            }
                                            else
                                            {
                                                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Call back for Task Id {0} on Processor {1} will NOT be invoked. Task status is no more BeingProcessed.", msgId, processor.Index);
                                            }
                                        }
                                        else
                                        {
                                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Task Id {0} status is {1} and cannot be processed on Processor {2}. might have timed out.", msgId, t.Status, processor.Index);
                                        }
                                    }
                                    else
                                    {
                                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Task for Task Id {0} does not exist on Processor {1}", msgId, processor.Index);
                                    }
                                }
                                else
                                {
                                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Task Id {0} does not exist on Processor {1}", msgId, processor.Index);
                                }
                            }
                            else
                            {
                                // either the message id is invalid or could not be extracted from the message...
                            }
                        }
                        else
                        {
                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Processor {0} did NOT received data", processor.Index);

                            var count = _queues[processor.Index].Count(ts => ts.Value.Status == (int)TaskStateStatus.ToProcess || ts.Value.Status == (int)TaskStateStatus.BeingProcessed);

                            if (count == 0)
                            {
                                processor.SuspendReceiveWaitingOnIO();
                            }
                            else
                            {
                                Thread.Sleep(50);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "An error occurred while processing RunReceive loop on Processor {0}. Exception is {1}", processor.Index, e);
                    }
                }
            }
        }

        private IAsyncResult BeginSend(AsyncCallback callback, object state)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "BeginSend"))
            {
                return QueueTask(callback, state);
            }
        }

        private string EndSend(IAsyncResult result)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "EndSend"))
            {
                TaskState t = result as TaskState;
                t.Status = (int)TaskStateStatus.MarkedForDeletion;

                return t.Response;
            }
        }

        /// <summary>
        /// Stamps the message string with a unique ID to correlate response to request and its task state object.
        /// </summary>
        /// <param name="msgId"></param>
        /// <param name="msg"></param>
        /// <returns></returns>
        private string AddMsgId(long msgId, string msg)
        {
            if (!string.IsNullOrEmpty(msg))
            {
                var arr = msg.Split(',');

                if (arr != null && arr.Length > 1)
                {
                    arr[2] = msgId.ToString();
                }

                string formattedMsg = null;

                for (int i = 0; i < arr.Length; i++)
                {
                    if (!string.IsNullOrEmpty(formattedMsg))
                    {
                        formattedMsg += ",";
                    }

                    formattedMsg += arr[i];
                }

                return formattedMsg;
            }

            return null;
        }

        /// <summary>
        /// Reads the unique ID from the response.
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        private long GetMsgId(string msg)
        {
            if (!string.IsNullOrEmpty(msg))
            {
                var arr = msg.Split(',');

                if (arr != null && arr.Length > 1)
                {
                    long msgId = -1;

                    if (long.TryParse(arr[2], out msgId))
                    {
                        return msgId;
                    }
                }
            }

            return -1;
        }

        /// <summary>
        /// Returns next ID
        /// </summary>
        /// <returns></returns>
        private long NewMsgId()
        {
            long msgId = Interlocked.Increment(ref _nextMsgId);
            return msgId;
        }

        /// <summary>
        /// Updates the task state status.
        /// </summary>
        /// <param name="status"></param>
        /// <param name="taskId"></param>
        private void SetTaskStatus(TaskStateStatus status, long taskId)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "SetTaskStatus"))
            {
                var index = (int)(taskId % _processorCount);

                if (_queues[index].ContainsKey(taskId))
                {
                    _queues[index][taskId].Status = (int)status;
                    if (status == TaskStateStatus.MarkedForDeletion)
                    {
                        _queues[index][taskId].MarkedForDeletion = DateTime.Now;
                    }
                }
            }
        }

        /// <summary>
        /// Queues up a task state. Uses a lock primitive to lock the message's associated queue.
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="message"></param>
        /// <returns></returns>
        private IAsyncResult QueueTask(AsyncCallback callback, object message)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "QueueTask"))
            {
                Message internalMsg = (Message)message;

                var t = new TaskState(internalMsg.Id, AddMsgId(internalMsg.Id, internalMsg.Body), internalMsg.Timeout, callback);

                var index = (int)(internalMsg.Id % _processorCount);

                lock (_queues[index])
                {
                    _queues[index].Add(internalMsg.Id, t);
                    t.QueuedUp = DateTime.Now;
                    _processors[index].SignalSendIO();
                    _processors[index].SignalReceiveIO();
                }

                return t;
            }
        }

        #endregion

        #region IIOPool APIs

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Task base single API returning a response message.
        /// </summary>
        /// <param name="msg">Request message</param>
        /// <param name="timeout">Maximum amount of time in milliseconds that the IO operation will wait to complete.</param>
        /// <returns></returns>
        public async Task<string> Send(string msg, int timeout)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Send"))
            {
                var msgId = NewMsgId();

                var m = new Message();
                m.Body = msg;
                m.Id = msgId;
                m.Timeout = timeout;

                var timeoutTask = Task.Delay(timeout);
                var t = Task.Factory.FromAsync<string>(BeginSend, EndSend, m);

                var anyTask = Task.WhenAny(new Task[2] { timeoutTask, t });

                await anyTask.ConfigureAwait(false);

                if (anyTask.Result == timeoutTask)
                {
                    SetTaskStatus(TaskStateStatus.MarkedForDeletion, msgId);
                    return null;
                }

                return await t.ConfigureAwait(false);
            }
        }

        #endregion

        #region Public methods

        public void Start()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Start"))
            {
                _queues = new Dictionary<int, Dictionary<long, TaskState>>();
                _processors = new List<IOUnitProcessor>();
                _timers = new Dictionary<int, Timer>();

                IOUnitProcessor p = null;

                // TODO: parametrize in ctor
                for (int i = 0; i < _processorCount; i++)
                {
                    _queues.Add(i, new Dictionary<long, TaskState>());

                    p = new IOUnitProcessor(i, _connectionTimeout, _waitSendTimeout, _waitReceiveTimeout, _minimumPeriodBeforeWaitOnSend, _minimumPeriodBeforeWaitOnReceive);
                    _processors.Add(p);
                    ThreadPool.QueueUserWorkItem(new WaitCallback(RunSend), p);
                    ThreadPool.QueueUserWorkItem(new WaitCallback(RunReceive), p);
                    _timers.Add(i, new Timer(new TimerCallback(RunCleanUp), p, _cleanUpTimerDueTime, _cleanUpTimerInterval));
                }
            }
        }

        #endregion
    }
}
