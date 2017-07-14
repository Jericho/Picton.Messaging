using Picton.Messaging.Logging;
using System;
using System.Threading;

namespace Picton.Messaging.Utils
{
	/// <summary>
	/// An improvement over System.Threading.SemaphoreSlim that allows you to dynamically increase and
	/// decrease the number of threads that can access a resource or pool of resources concurrently.
	/// </summary>
	/// <seealso cref="System.Threading.SemaphoreSlim" />
	public class SemaphoreSlimEx : SemaphoreSlim
	{
		#region FIELDS

		private static readonly ILog _logger = LogProvider.GetLogger(typeof(SemaphoreSlimEx));
		private readonly ReaderWriterLockSlim _lock;

		#endregion

		#region PROPERTIES

		/// <summary>
		/// Gets the minimum number of slots.
		/// </summary>
		/// <value>
		/// The minimum slots count.
		/// </value>
		public int MinimumSlotsCount { get; private set; }

		/// <summary>
		/// Gets the number of slots currently available.
		/// </summary>
		/// <value>
		/// The available slots count.
		/// </value>
		public int AvailableSlotsCount { get; private set; }

		/// <summary>
		/// Gets the maximum number of slots.
		/// </summary>
		/// <value>
		/// The maximum slots count.
		/// </value>
		public int MaximumSlotsCount { get; private set; }

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// Initializes a new instance of the <see cref="SemaphoreSlimEx"/> class.
		/// </summary>
		/// <param name="minCount">The minimum number of slots.</param>
		/// <param name="initialCount">The initial number of slots.</param>
		/// <param name="maxCount">The maximum number of slots.</param>
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

		/// <summary>
		/// Attempts to increase the number of slots
		/// </summary>
		/// <param name="millisecondsTimeout">The timeout in milliseconds.</param>
		/// <returns>true if the attempt was successfully; otherwise, false.</returns>
		public bool TryIncrease(int millisecondsTimeout = 500)
		{
			return TryIncrease(TimeSpan.FromMilliseconds(millisecondsTimeout));
		}

		/// <summary>
		/// Attempts to increase the number of slots
		/// </summary>
		/// <param name="timeout">The timeout.</param>
		/// <returns>true if the attempt was successfully; otherwise, false.</returns>
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
							Release();
							this.AvailableSlotsCount++;
							increased = true;
							_logger.Trace($"Semaphore slots increased: {this.AvailableSlotsCount}");
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

		/// <summary>
		/// Attempts to decrease the number of slots
		/// </summary>
		/// <param name="millisecondsTimeout">The timeout in milliseconds.</param>
		/// <returns>true if the attempt was successfully; otherwise, false.</returns>
		public bool TryDecrease(int millisecondsTimeout = 500)
		{
			return TryDecrease(TimeSpan.FromMilliseconds(millisecondsTimeout));
		}

		/// <summary>
		/// Attempts to decrease the number of slots
		/// </summary>
		/// <param name="timeout">The timeout.</param>
		/// <returns>true if the attempt was successfully; otherwise, false.</returns>
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
						if (Wait(timeout))
						{
							this.AvailableSlotsCount--;
							decreased = true;
							_logger.Trace($"Semaphore slots decreased: {this.AvailableSlotsCount}");
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
