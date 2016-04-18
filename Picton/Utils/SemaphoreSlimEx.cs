using System;
using System.Threading;

namespace Picton.Utils
{
	public class SemaphoreSlimEx : SemaphoreSlim
	{
		private ReaderWriterLockSlim _lock;

		public int MinimumSlotsCount { get; private set; }
		public int AvailableSlotsCount { get; private set; }
		public int MaximumSlotsCount { get; private set; }

		public SemaphoreSlimEx(int minCount, int initialCount, int maxCount)
			: base(initialCount, maxCount)
		{
			_lock = new ReaderWriterLockSlim(LockRecursionPolicy.NoRecursion);

			this.MinimumSlotsCount = minCount;
			this.AvailableSlotsCount = initialCount;
			this.MaximumSlotsCount = maxCount;

		}

		public bool TryIncrease(int timeout = 500)
		{
			return TryIncrease(TimeSpan.FromMilliseconds(timeout));
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

		public bool TryDecrease(int timeout = 500)
		{
			return TryDecrease(TimeSpan.FromMilliseconds(timeout));
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
						}
					}
					_lock.ExitWriteLock();
				}
			}
			return decreased;
		}
	}
}
