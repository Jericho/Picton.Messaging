using System.Threading;

namespace Picton.Azure.Utils
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
			this.MinimumSlotsCount = minCount;
			this.AvailableSlotsCount = initialCount;
			this.MaximumSlotsCount = maxCount;
			_lock = Locks.GetLockInstance();
		}

		public bool TryIncrease()
		{
			var increased = false;
			try
			{
				using (new ReadLock(_lock))
				{
					if (this.AvailableSlotsCount < this.MaximumSlotsCount)
					{
						using (new WriteLock(_lock))
						{
							if (this.AvailableSlotsCount < this.MaximumSlotsCount)
							{
								using (new WriteLock(_lock))
								{
									base.Release();
									this.AvailableSlotsCount++;
								}
								increased = true;
							}
						}
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
			var decreased = false;

			using (new ReadLock(_lock))
			{
				if (this.AvailableSlotsCount > this.MinimumSlotsCount)
				{
					using (new WriteLock(_lock))
					{
						if (this.AvailableSlotsCount > this.MinimumSlotsCount)
						{
							if (base.Wait(timeout))
							{
								this.AvailableSlotsCount--;
								decreased = true;
							}
						}
					}
				}
			}
			return decreased;
		}
	}
}