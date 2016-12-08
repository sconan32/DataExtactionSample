
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;

namespace DataExtactionSample
{
    public class BufferManager<T>
    {
        private readonly int m_ByteSize;

        private readonly Stack<T> m_Buffers;
        private readonly object m_LockObject = new Object();      
        
        #region constructors

        public BufferManager(IEnumerable<T> collection)
        {
            lock (m_LockObject)
            {
                m_Buffers = new Stack<T>(collection);

            }
        }

        #endregion //constructors

        public int AvailableBuffers
        {
            get { return m_Buffers.Count; }
        }


            



        /// <summary>
        /// Checks out a buffer from the manager
        /// </summary>        
        public T CheckOut()
        {
            lock (m_LockObject)
            {
                if (m_Buffers.Count == 0)
                {
                    Debug.WriteLine("POOL IS EMPTY!");
                    return default(T);

                }
                return m_Buffers.Pop();
            }
        }


        /// <summary>
        /// Returns a buffer to the control of the manager
        /// </summary>
        ///<remarks>
        /// It is the Client’s responsibility to return the buffer to the manger by
        /// calling Checkin on the buffer
        ///</remarks>
        public void CheckIn(T _Buffer)
        {
            lock (m_LockObject)
            {
                m_Buffers.Push(_Buffer);
            }
        }


    }
}