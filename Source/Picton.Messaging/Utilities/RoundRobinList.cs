using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

namespace Picton.Messaging.Utilities
{
	internal class RoundRobinList<T>
	{
		private static readonly ReaderWriterLockSlim _lock = new ReaderWriterLockSlim();
		private readonly LinkedList<T> _linkedList;
		private LinkedListNode<T> _current;

		/// <summary>
		/// Initializes a new instance of the <see cref="RoundRobinList{T}"/> class.
		/// </summary>
		/// <param name="list">The items.</param>
		public RoundRobinList(IEnumerable<T> list)
		{
			_linkedList = new LinkedList<T>(list);
		}

		public T Current
		{
			get
			{
				try
				{
					_lock.EnterReadLock();

					return _current == default ? default : _current.Value;
				}
				finally
				{
					if (_lock.IsReadLockHeld) _lock.ExitReadLock();
				}
			}
		}

		public T Next
		{
			get
			{
				try
				{
					_lock.EnterReadLock();

					return (_current == default || _current.Next == default) ? _linkedList.First.Value : _current.Next.Value;
				}
				finally
				{
					if (_lock.IsReadLockHeld) _lock.ExitReadLock();
				}
			}
		}

		public T Previous
		{
			get
			{
				try
				{
					_lock.EnterReadLock();

					return (_current == default || _current.Previous == default) ? _linkedList.Last.Value : _current.Previous.Value;
				}
				finally
				{
					if (_lock.IsReadLockHeld) _lock.ExitReadLock();
				}
			}
		}

		public int Count
		{
			get
			{
				try
				{
					_lock.EnterReadLock();

					return _linkedList.Count;
				}
				finally
				{
					if (_lock.IsReadLockHeld) _lock.ExitReadLock();
				}
			}
		}

		/// <summary>
		/// Reset the Round Robin to point to the first item.
		/// </summary>
		public void Reset()
		{
			try
			{
				_lock.EnterWriteLock();
				_current = _linkedList.First;
			}
			finally
			{
				if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
			}
		}

		/// <summary>
		/// Reset the Round Robin to point to the specified item.
		/// </summary>
		public void ResetTo(T item)
		{
			try
			{
				_lock.EnterWriteLock();
				_current = _linkedList.Find(item);
			}
			finally
			{
				if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
			}
		}

		/// <summary>
		/// Reset the Round Robin to point to the item at the specified index.
		/// </summary>
		public void ResetTo(int index)
		{
			try
			{
				_lock.EnterWriteLock();
				_current = _linkedList.Find(_linkedList.ElementAt(index));
			}
			finally
			{
				if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
			}
		}

		/// <summary>
		/// Move to the next item in the list.
		/// </summary>
		/// <returns>The item.</returns>
		public T MoveToNextItem()
		{
			try
			{
				_lock.EnterUpgradeableReadLock();

				if (_linkedList.Count == 0) throw new InvalidOperationException("List is empty.");

				try
				{
					_lock.EnterWriteLock();

					if (_linkedList.Count == 0) throw new InvalidOperationException("List is empty.");
					_current = _current == null ? _linkedList.First : _current.Next ?? _current.List.First;
				}
				finally
				{
					if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
				}
			}
			finally
			{
				if (_lock.IsUpgradeableReadLockHeld) _lock.ExitUpgradeableReadLock();
			}

			return _current.Value;
		}

		/// <summary>
		/// Remove an item from the list.
		/// </summary>
		/// <returns>The item.</returns>
		public bool RemoveItem(T item)
		{
			try
			{
				_lock.EnterWriteLock();

				return _linkedList.Remove(item);
			}
			finally
			{
				if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
			}
		}

		public void AddItem(T item)
		{
			try
			{
				_lock.EnterUpgradeableReadLock();

				if (!_linkedList.Contains(item))
				{
					try
					{
						_lock.EnterWriteLock();

						if (!_linkedList.Contains(item))
						{
							_linkedList.AddLast(item);
						}
					}
					finally
					{
						if (_lock.IsWriteLockHeld) _lock.ExitWriteLock();
					}
				}
			}
			finally
			{
				if (_lock.IsUpgradeableReadLockHeld) _lock.ExitUpgradeableReadLock();
			}
		}
	}
}
