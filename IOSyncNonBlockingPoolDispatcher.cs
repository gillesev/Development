using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;


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
    public class IOSyncNonBlockingUnitProcessor : IIOSyncNonBlockingUnitProcessor
    {
        private const string CLASS_NAME = "IOSyncNonBlockingUnitProcessor";

        #region Private methods

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
        /// Updates the task state status.
        /// </summary>
        /// <param name="status"></param>
        /// <param name="taskId"></param>
        private void SetTaskStatus(TaskStateStatus status, long taskId)
        {
            if (_queue.ContainsKey(taskId))
            {
                _queue[taskId].Status = (int)status;
                if (status == TaskStateStatus.MarkedForDeletion)
                {
                    _queue[taskId].MarkedForDeletion = DateTime.Now;
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

                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Queuing Task Id={0} in processor {1}", internalMsg.Id, this.Index);

                var t = new TaskState(internalMsg.Id, AddMsgId(internalMsg.Id, internalMsg.Body), internalMsg.Timeout, callback);

                lock (_queue)
                {
                    _queue.Add(internalMsg.Id, t);
                    t.QueuedUp = DateTime.Now;
                    SignalSendIO();
                    SignalReceiveIO();
                }

                return t;
            }
        }

        /// <summary>
        /// Old APM pattern BeginXXX method
        /// </summary>
        /// <param name="callback"></param>
        /// <param name="state"></param>
        /// <returns></returns>
        private IAsyncResult BeginSend(AsyncCallback callback, object state)
        {
            return QueueTask(callback, state);
        }

        /// <summary>
        /// Old APM pattern EndXXX method
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        private string EndSend(IAsyncResult result)
        {
            TaskState t = result as TaskState;
            t.Status = (int)TaskStateStatus.MarkedForDeletion;

            return t.Response;
        }

        /// <summary>
        /// Executes a cleanup cycle.
        /// </summary>
        /// <param name="p"></param>
        private void RunCleanUp(object p)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "RunCleanUp"))
            {
                // stop the timer
                _timer.Change(Timeout.Infinite, _cleanUpTimerInterval);

                int count = _queue.Count(ts => ts.Value.Status == (int)TaskStateStatus.MarkedForDeletion);

                if (count > _minimumMarkedForDeletionBeforeGCed)
                {
                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Scheduling a GC cyle to delete {0} Tasks. Minium set={1} on processor {2}.", count, _minimumMarkedForDeletionBeforeGCed, this.Index);

                    // signal intent to clean-up
                    this.SignalIntentToCleanUp(true);

                    // wait signal from both the Send/Receive threads to start
                    this.WaitToCleanUp();

                    var dic = _queue;

                    var copy = dic.ToList();

                    var en = copy.GetEnumerator();

                    while (en.MoveNext())
                    {
                        dic.Remove(en.Current.Value.TaskId);
                    }
                }

                _timer.Change(_cleanUpTimerInterval, _cleanUpTimerInterval);

                // signal both threads they can resume...
                this.SignalIntentToCleanUp(false);
                this.SignalSendWithCleanUpCompleteAck();
                this.SignalReceiveWithCleanUpCompleteAck();
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

                // if nb of tasks in ToProcess status, wait on mre
                // when a task is queued up, signal mre...
                // when done with processing the set of tasks in ToProcess status, reset mre...
                while (true)
                {
                    try
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "RunSend loop started on Processor {0}", this.Index);

                        if (this.IntentToCleanUpSignaled)
                        {
                            this.SignalCleanUpWithSendAck();

                            this.SuspendSendWaitingOnCleanUp();
                        }

                        // by this time the IntentToCleanUpSignaled should have been reset to FALSE.
                        listToProcess = null;
                        lock (_queue)
                        {
                            try
                            {
                                listToProcess = _queue.Select(kvp => kvp.Value).Where(task => task.Status == (int)TaskStateStatus.ToProcess).ToList();
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
                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "RunSend loop found {0} tasks to process on Processor {1}", listToProcess.Count, this.Index);

                            foreach (var task in listToProcess)
                            {
                                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Sending data for Task Id {0} on Processor {1}", task.TaskId, this.Index);

                                if (DateTime.Now <= task.CreationDateTime.Add(new TimeSpan(0, 0, 0, 0, task.Timeout)))
                                {
                                    if (this.Connect() == 0)
                                    {
                                        if (this.Send(task.AsyncState.ToString()) != 0)
                                        {
                                            // TODO: error in sending
                                            // need to return error to caller.
                                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "FAILED sending Task Id {0} on Processor {1}", task.TaskId, this.Index);
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
                                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "FAILED sending Task Id {0} on Processor {1}. Connect FAILED.", task.TaskId, this.Index);
                                        task.Status = (int)TaskStateStatus.ToProcess;
                                    }
                                }
                                else
                                {
                                    task.Status = (int)TaskStateStatus.Error;

                                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "TIMED OUT sending Task Id {0} on Processor {1}", task.TaskId, this.Index);
                                }
                            }
                        }
                        else
                        {
                            if (!this.SuspendSendWaitingOnIO())
                            {
                                Thread.Sleep(50);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "An error occurred while processing RunSend loop on Processor {0}. Exception is {1}", this.Index, e);
                    }
                }
            }
        }

        private void InvokeCallBack(object state)
        {
            TaskState t = state as TaskState;

            t.CallBack.Invoke(t);
        }

        /// <summary>
        /// Executes the infinite loop run on the Receive dedicated thread pool thread.
        /// </summary>
        /// <param name="p"></param>
        private void RunReceive(object p)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "RunReceive"))
            {
                while (true)
                {
                    try
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "RunReceive loop started on Processor {0}", this.Index);

                        if (this.IntentToCleanUpSignaled)
                        {
                            this.SignalCleanUpWithReceiveAck();

                            this.SuspendReceiveWaitingOnCleanUp();
                        }

                        TaskState t = null;
                        long msgId = 0;
                        string resp = null;

                        // poll the processor to check if it did receive a response
                        if (this.Received)
                        {
                            resp = this.Read();

                            msgId = GetMsgId(resp);

                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Read data for Task Id {0} on Processor {1}", msgId, this.Index);

                            if (msgId >= 0)
                            {
                                if (_queue.ContainsKey(msgId))
                                {
                                    t = _queue[msgId];

                                    if (t != null)
                                    {
                                        if (t.Status == (int)TaskStateStatus.BeingProcessed && DateTime.Now <= t.CreationDateTime.Add(new TimeSpan(0, 0, 0, 0, t.Timeout)))
                                        {
                                            lock (_queue)
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

                                            if (t != null)
                                            {
                                                t.Response = resp;

                                                // dispatch continuation work on a different thread pool.
                                                // this mechanism is equivalent to a IO completion port thread executing the continuation of the work.
                                                // reason is cannot tie this dedicated work to execute the continuation.
                                                ThreadPool.QueueUserWorkItem(new WaitCallback(InvokeCallBack), t);
                                            }
                                            else
                                            {
                                                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Call back for Task Id {0} on Processor {1} will NOT be invoked. Task status {2} is no more [BeingProcessed].", msgId, t.Status, this.Index);
                                            }
                                        }
                                        else
                                        {
                                            trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Task Id {0} status is {1} and cannot be processed on Processor {2}. Might have timed out.", msgId, t.Status, this.Index);
                                        }
                                    }
                                    else
                                    {
                                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Task Id {0} cannot be found on Processor {1}'s queue.", msgId, this.Index);
                                    }
                                }
                                else
                                {
                                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Task Id {0} cannot be found on Processor {1} has an invalid Id.", msgId, this.Index);
                                }
                            }
                            else
                            {
                                // either the message id is invalid or could not be extracted from the message...
                            }
                        }
                        else
                        {
                            var count = _queue.Count(ts => ts.Value.Status == (int)TaskStateStatus.ToProcess || ts.Value.Status == (int)TaskStateStatus.BeingProcessed);

                            if (count == 0)
                            {
                                this.SuspendReceiveWaitingOnIO();
                            }
                            else
                            {
                                Thread.Sleep(50);
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "An error occurred while processing RunReceive loop on Processor {0}. Exception is {1}", this.Index, e);
                    }
                }
            }
        }

        /// <summary>
        /// Synchronous Connect using the connection timeout.
        /// </summary>
        /// <returns></returns>
        private int Connect()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Connect", "Processor {0}", this.Index))
            {
                if (_connection.Connect() == true)
                {
                    var timeoutDT = DateTime.Now.Add(new TimeSpan(0, 0, 0, 0, _connectionTimeout));

                    while (string.IsNullOrEmpty(_connection.Current_ISP_Connected) && DateTime.Now < timeoutDT)
                    {
                    }

                    if (string.IsNullOrEmpty(_connection.Current_ISP_Connected))
                        trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Connection for processor {0} failed after waiting for {1} msec.", this.Index, this._connectionTimeout);

                    return !string.IsNullOrEmpty(_connection.Current_ISP_Connected) ? 0 : -1;
                }

                return -1;
            }
        }

        /// <summary>
        /// Synchronous Disconnect
        /// </summary>
        /// <returns></returns>
        private int Disconnect()
        {
            if (_connection != null)
            {
                _connection.Disconnect();
            }

            return 0;
        }

        /// <summary>
        /// Unique identifier for the processor. Used to correlate a queue and its processor.
        /// </summary>
        private int Index
        {
            get { return _index; }
        }

        /// <summary>
        /// Signals the Send thread for new IO task
        /// </summary>
        private void SignalSendIO()
        {
            _mreSend.Set();
            _lastSignaledDateTime = DateTime.Now;
        }

        /// <summary>
        /// Suspends the Send thread until new IO task is queued up.
        /// </summary>
        /// <returns></returns>
        private bool SuspendSendWaitingOnIO()
        {
            if (!_lastSignaledDateTime.HasValue || _lastSignaledDateTime < DateTime.Now.Subtract(new TimeSpan(0, 0, 0, 0, _minimumPeriodBeforeWaitOnSend)))
            {
                _mreSend.WaitOne(_waitSendTimeout);
                return true;
            }

            return false;
        }

        /// <summary>
        /// Signals the Receive thread for new IO task
        /// </summary>
        private void SignalReceiveIO()
        {
            _mreReceive.Set();
        }

        /// <summary>
        /// Suspend Receive thread until new IO task is queued up.
        /// </summary>
        /// <returns></returns>
        private bool SuspendReceiveWaitingOnIO()
        {
            if (!_lastSignaledDateTime.HasValue || _lastSignaledDateTime < DateTime.Now.Subtract(new TimeSpan(0, 0, 0, 0, _minimumPeriodBeforeWaitOnReceive)))
            {
                return _mreReceive.WaitOne(_waitReceiveTimeout);
            }
            Thread.Sleep(50);
            return false;
        }

        /// <summary>
        /// Signals an intent to perform cleanup (called by cleanup timer thread)
        /// </summary>
        /// <param name="doCleanUp"></param>
        private void SignalIntentToCleanUp(bool doCleanUp)
        {
            Interlocked.Exchange(ref _intentToCleanUp, (doCleanUp) ? 1 : 0);
        }

        /// <summary>
        /// Returns TRUE if an intent to perform some cleanup has been set by the cleanup timer thread.
        /// </summary>
        private bool IntentToCleanUpSignaled
        {
            get { return _intentToCleanUp == 1; }
        }

        /// <summary>
        /// Suspend the cleanup thread until signaled by both the Send and Receive threads
        /// </summary>
        /// <returns></returns>
        private bool WaitToCleanUp()
        {
            return WaitHandle.WaitAll(new WaitHandle[2] { _mreSendAckIntentToCleanup, _mreReceiveAckIntentToCleanup });
        }

        /// <summary>
        /// Signals the cleanup thread that the Send thread has acknowledge the intent to cleanup
        /// </summary>
        private void SignalCleanUpWithSendAck()
        {
            _mreSendAckIntentToCleanup.Set();
        }

        /// <summary>
        /// Signals the cleanup thread that the Receive thread has acknowledge the intent to cleanup
        /// </summary>
        private void SignalCleanUpWithReceiveAck()
        {
            _mreReceiveAckIntentToCleanup.Set();
        }

        /// <summary>
        /// Signals the Send thread that the cleanup is complete
        /// </summary>
        private void SignalSendWithCleanUpCompleteAck()
        {
            _mreSignalSendAckCleanUpComplete.Set();
        }

        /// <summary>
        /// Signals the Receive thread that the cleanup is complete
        /// </summary>
        private void SignalReceiveWithCleanUpCompleteAck()
        {
            _mreSignalReceiveAckCleanUpComplete.Set();
        }

        /// <summary>
        /// Suspend forever the Send thread until the cleanup thread signals and frees it.
        /// </summary>
        private void SuspendSendWaitingOnCleanUp()
        {
            _mreSignalSendAckCleanUpComplete.WaitOne();
        }

        /// <summary>
        /// Suspend forever the Send thread until the cleanup thread signals and frees it.
        /// </summary>
        private void SuspendReceiveWaitingOnCleanUp()
        {
            _mreSignalReceiveAckCleanUpComplete.WaitOne();
        }

        /// <summary>
        /// Clean up resources
        /// </summary>
        /// <param name="disposing"></param>
        private void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    if (_connection != null)
                    {
                        _connection.Disconnect();
                    }
                }

                _connection = null;

                _disposed = true;
            }
        }

        #endregion

        #region IIOUnitProcessor APIs

        public async Task<string> Send(long msgId, string msg, int timeout)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Send"))
            {
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
                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Processing of message Task Id={0} TIMED OUT.", m.Id);
                    SetTaskStatus(TaskStateStatus.MarkedForDeletion, msgId);
                    return null;
                }

                return await t.ConfigureAwait(false);
            }
        }

        #endregion

        #region IIOSyncNonBlockingUnitProcessor APIs

        /// <summary>
        /// Synchronous Send
        /// </summary>
        /// <param name="msg"></param>
        /// <returns></returns>
        public int Send(string msg)
        {
            if (_connection != null)
                return _connection.Send(msg) == true ? 0 : -1;

            return -1;
        }

        /// <summary>
        /// Synchronous Read
        /// </summary>
        /// <returns></returns>
        public string Read()
        {
            if (_connection != null)
                return _connection.Read();

            return null;
        }

        /// <summary>
        /// True if Data has been received
        /// </summary>
        public bool Received 
        {
            get 
            {
                if (_connection != null)
                    return _connection.DataAvailable > 0 ? true : false;

                return false;
            } 
        }

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        #endregion

        #region Public Methods

        public void Start()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Start"))
            {
                ThreadPool.QueueUserWorkItem(new WaitCallback(RunSend), null);
                ThreadPool.QueueUserWorkItem(new WaitCallback(RunReceive), null);
                _timer = new Timer(new TimerCallback(RunCleanUp), null, _cleanUpTimerDueTime, _cleanUpTimerInterval);                
            }
        }

        #endregion

        #region Private Members

        private Dictionary<long, TaskState> _queue;
        private Timer _timer;
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
        private int _cleanUpTimerDueTime;
        private int _cleanUpTimerInterval;
        private int _minimumMarkedForDeletionBeforeGCed;

        private AutoResetEvent _mreSignalSendAckCleanUpComplete;
        private AutoResetEvent _mreSignalReceiveAckCleanUpComplete;
        private bool _disposed;

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
        public IOSyncNonBlockingUnitProcessor(int index, int connectionTimeout, int waitSendTimeout, int waitReceiveTimeout, int minimumPeriodBeforeWaitOnSend, int minimumPeriodBeforeWaitOnReceive, int cleanUpTimerDueTime, int cleanUpTimerInterval, int minimumMarkedForDeletionBeforeGCed)
        {
            _queue = new Dictionary<long, TaskState>();
            _index = index;
            _connection = new XXX(PORT, IP, false);
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
            _cleanUpTimerDueTime = cleanUpTimerDueTime;
            _cleanUpTimerInterval = cleanUpTimerInterval;
            _minimumMarkedForDeletionBeforeGCed = minimumMarkedForDeletionBeforeGCed;
            _mreSignalSendAckCleanUpComplete = new AutoResetEvent(false);
            _mreSignalReceiveAckCleanUpComplete = new AutoResetEvent(false);
        }

        #endregion
    }

    public class IOSyncNonBlockingPoolDispatcher : IIODispatcher
    {
        private const string CLASS_NAME = "IOSyncNonBlockingPoolDispatcher";

        #region Private Members

        /// <summary>
        /// Nb of unit processors to spawn
        /// </summary>
        private int _processorCount = 1;

        /// <summary>
        /// Timeout to connect to 3nd party component
        /// </summary>
        private int _connectionTimeout = 2000;

        /// <summary>
        /// Maximum amount of time that a unit processor Send thread will wait before waking up
        /// </summary>
        private int _waitSendTimeout = 30000;

        /// <summary>
        /// Maximum amount of time that a unit processor Receive thread will wait before waking up
        /// </summary>
        private int _waitReceiveTimeout = 30000;

        /// <summary>
        /// Amount of time that a unit processor's cleanup timer will wait before starting
        /// </summary>
        private int _cleanUpTimerDueTime = 30000;

        /// <summary>
        /// Interval of time at which a unit processor's cleanup timer will tick
        /// </summary>
        private int _cleanUpTimerInterval = 30000;

        /// <summary>
        /// Minimum amount of time elapsed since last time an IO message was queued up for a unit processor's Send thread
        /// </summary>
        private int _minimumPeriodBeforeWaitOnSend = 10000;

        /// <summary>
        /// Minimum amount of time elapsed since last time an IO message was queued up for a unit processor's Receive thread
        /// </summary>
        private int _minimumPeriodBeforeWaitOnReceive = 10000;

        /// <summary>
        /// Minium number of tasks that need to be marked for deletion before being considered by a unit processor's Cleanup cycle.
        /// </summary>
        private int _minimumMarkedForDeletionBeforeGCed = 100;

        /// <summary>
        /// collection of unit processors.
        /// </summary>
        private List<IOSyncNonBlockingUnitProcessor> _processors;

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
        public IOSyncNonBlockingPoolDispatcher(int processorCount, int connectionTimeout, int waitSendTimeout, int waitReceiveTimeout, int minimumPeriodBeforeWaitOnSend, int minimumPeriodBeforeWaitOnReceive, int cleanUpTimerDueTime, int cleanUpTimerInterval, int minimumMarkedForDeletionBeforeGCed)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Ctor"))
            {
                trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Dispatcher settings. Processor Count={0}. Connection Timeout={1}. WaitSendTimeout={2}. Wait Receive Timeout={3}. Min Duration Before Waiting on Send={4}. Min Duration Before Waiting on Receive={5}. Cleanup Timer Due Duration={6}. Cleanup Timer Interval={7}. Min count marked for deltion Before GC cycle is scheduled={8}.",
                    processorCount,
                    connectionTimeout,
                    waitSendTimeout,
                    waitReceiveTimeout,
                    minimumPeriodBeforeWaitOnSend,
                    minimumPeriodBeforeWaitOnReceive,
                    cleanUpTimerDueTime,
                    cleanUpTimerInterval,
                    minimumMarkedForDeletionBeforeGCed);

                this._processorCount = processorCount;
                this._connectionTimeout = connectionTimeout;
                this._waitSendTimeout = waitSendTimeout;
                this._waitReceiveTimeout = waitReceiveTimeout;
                this._cleanUpTimerDueTime = cleanUpTimerDueTime;
                this._cleanUpTimerInterval = cleanUpTimerInterval;
                this._minimumPeriodBeforeWaitOnSend = minimumPeriodBeforeWaitOnSend;
                this._minimumPeriodBeforeWaitOnReceive = minimumPeriodBeforeWaitOnReceive;
                this._minimumMarkedForDeletionBeforeGCed = minimumMarkedForDeletionBeforeGCed;

                _processors = new List<IOSyncNonBlockingUnitProcessor>();
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
                }

                _processors = null;

                _disposed = true;
            }
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

        #endregion

        #region IIOPool APIs

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispatches the message to a pool of unit processors
        /// </summary>
        /// <param name="msg">Request message</param>
        /// <param name="timeout">Maximum amount of time in milliseconds that the IO operation will wait to complete the task.</param>
        /// <returns></returns>
        public async Task<string> Send(string msg, int timeout)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Send"))
            {
                // The message id will be used to correlate a request with a response which is KEY to transform the synchronous/non blocking API pattern into a request/response API pattern.
                // Note that this Id is internal BUT it is a required pre-requisite for this synchronous/non blocking API pattern to function.
                // without the capability to mark/stamp the message provided to the 3nd party library, no correlation would be possible.
                var msgId = NewMsgId();

                // this is how the dispatching is implemented.
                // msg.mod(_processorCount) gives you the unit processor in charge of processing this message.
                // note that this is a VERY simple round-robin type of dispatching...
                var index = (msgId % _processorCount);

                IOSyncNonBlockingUnitProcessor p = _processors[(int)index];

                // dispatch the message to the unit processor identified to process this message.
                return await p.Send(msgId, msg, timeout).ConfigureAwait(false);
            }
        }

        #endregion

        #region Public methods

        public void Start()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Start"))
            {
                IOSyncNonBlockingUnitProcessor p = null;

                for (int i = 0; i < _processorCount; i++)
                {
                    p = new IOSyncNonBlockingUnitProcessor(i, _connectionTimeout, _waitSendTimeout, _waitReceiveTimeout, _minimumPeriodBeforeWaitOnSend, _minimumPeriodBeforeWaitOnReceive, _cleanUpTimerDueTime, _cleanUpTimerInterval, _minimumMarkedForDeletionBeforeGCed);
                    _processors.Add(p);
                    p.Start();
                }
            }
        }

        #endregion
    }

    public class Dispatcher : IIODispatcher
    {
        private const string CLASS_NAME = "Dispatcher";

        #region Private Members

        private static Dispatcher _p = new Dispatcher();
        private IOSyncNonBlockingPoolDispatcher __p;
        private bool _disposed;
        private string _DEVStoreId;

        #endregion

        #region Ctor

        private Dispatcher()
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Ctor"))
            {
                __p = new IOSyncNonBlockingPoolDispatcher(1, 2000, 30000, 30000, 10000, 10000, 30000, 30000, 100);

                __p.Start();

                // read DEVELOPMENT
                _DEVStoreId = IssRetail.ConfigurationSvc.IssConfigurationSvc.getInstance().ReadConfigurationValue("DEVSTOREID");

                if (!string.IsNullOrEmpty(_DEVStoreId))
                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "Virtual Development Store Id={0}.", _DEVStoreId);
                else
                    trace.Log(LogGroupTypeEnum.Processing_Step, LogSeverityEnum.Debug_Trace, "No Virtual Development Store Id setup.");
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
                    if (__p != null)
                    {
                        __p.Dispose();
                        __p = null;
                    }
                    _disposed = true;
                }
            }
        }

        private string ReplaceStoreId(string msg, string newStoreId, out string oldStoreId)
        {
            var arr = msg.Split(',');

            oldStoreId = null;

            if (!string.IsNullOrEmpty(newStoreId) && arr != null && arr.Length > 7)
            {
                // IxStoreNumber
                oldStoreId = arr[7];
                arr[7] = newStoreId;

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
            else
            {
                return msg;
            }
        }

        #endregion

        #region Public Members

        public static Dispatcher Instance
        {
            get { return _p; }
        }

        #endregion

        #region IIODispatcher APIs

        public async Task<string> Send(string msg, int timeout)
        {
            using (var trace = Tracer.Create(CLASS_NAME, "Send"))
            {
                string originalStoreId = null;
                string formattedMsg = null;
                bool storeIdReplaced = false;

                if (!string.IsNullOrEmpty(_DEVStoreId))
                {
                    formattedMsg = ReplaceStoreId(msg, _DEVStoreId, out originalStoreId);
                    storeIdReplaced = true;
                }
                else
                {
                    formattedMsg = msg;
                }

                var resp = await __p.Send(formattedMsg, timeout).ConfigureAwait(false);

                if (!string.IsNullOrEmpty(resp) && storeIdReplaced)
                {
                    return ReplaceStoreId(resp, originalStoreId, out originalStoreId);
                }
                else
                {
                    return resp;
                }
            }
        }

        public void Dispose()
        {
            Dispose(true);

            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
