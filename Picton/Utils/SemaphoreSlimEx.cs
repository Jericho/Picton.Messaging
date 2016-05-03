using Picton.Logging;
using System;
using System.Threading;

namespace Picton.Utils
{
	public class SemaphoreSlimEx : SemaphoreSlim
	{
		#region FIELDS

		private ReaderWriterLockSlim _lock;
		private static readonly ILog _logger = LogProvider.GetCurrentClassLogger();

		#endregion

		#region PROPERTIES

		public int MinimumSlotsCount { get; private set; }
		public int AvailableSlotsCount { get; private set; }
		public int MaximumSlotsCount { get; private set; }

		#endregion

		#region CONSTRUCTOR

		public SemaphoreSlimEx(int minCount, int initialCount, int maxCount)
			: base(initialCount, maxCount)
		{
			_lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

			this.MinimumSlotsCount = minCount;
			this.AvailableSlotsCount = initialCount;
			this.MaximumSlotsCount = maxCount;

		}

		#endregion

		#region PUBLIC METHODS

		public bool TryIncrease(int millisecondsTimeout = 500)
		{
			return TryIncrease(TimeSpan.FromMilliseconds(millisecondsTimeout));
		}

		public bool TryIncrease(TimeSpan timeout)
		{
			var increased = false;
			try
			{
				if (this.AvailableSlotsCount < this.MaximumSlotsCount)
				{
					var lockAcquired = _lock.TryEnterWriteLock(timeout);
					if (lockAcquired)
					{
						if (this.AvailableSlotsCount < this.MaximumSlotsCount)
						{
							base.Release();
							this.AvailableSlotsCount++;
							increased = true;
							_logger.Trace(string.Format("Semaphone slots increased: {0}", this.AvailableSlotsCount));
						}
						_lock.ExitWriteLock();
					}
				}
			}
			catch (SemaphoreFullException)
			{
				// An exception is thrown if we attempt to exceed the max number of concurrent tasks
				// It's safe to ignore this exception
			}
			return increased;
		}

		public bool TryDecrease(int millisecondsTimeout = 500)
		{
			return TryDecrease(TimeSpan.FromMilliseconds(millisecondsTimeout));
		}

		public bool TryDecrease(TimeSpan timeout)
		{
			var decreased = false;

			if (this.AvailableSlotsCount > this.MinimumSlotsCount)
			{
				var lockAcquired = _lock.TryEnterWriteLock(timeout);
				if (lockAcquired)
				{
					if (this.AvailableSlotsCount > this.MinimumSlotsCount)
					{
						if (base.Wait(timeout))
						{
							this.AvailableSlotsCount--;
							decreased = true;
							_logger.Trace(string.Format("Semaphone slots decreased: {0}", this.AvailableSlotsCount));
						}
					}
					_lock.ExitWriteLock();
				}
			}
			return decreased;
		}

		#endregion	
	}
}
