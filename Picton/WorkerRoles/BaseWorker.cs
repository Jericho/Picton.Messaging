using Microsoft.WindowsAzure.ServiceRuntime;
using System.Diagnostics;
using System.Linq;
using System.Net;

namespace Picton.WorkerRoles
{
	public abstract class BaseWorker : RoleEntryPoint
	{
		#region PROPERTIES

		public string WorkerName { get; private set; }

		#endregion

		#region CONSTRUCTOR

		/// <summary>
		/// Constructor
		/// </summary>
		/// <param name="workerName">
		/// The name must match the name of the role in Azure. For example: WorkerRoleCqrsEmailMarketingHistoricalActivities.
		/// This is particularly important for auto-scaling. If the name is incorrect, the auto-scaling will either fail to be
		/// setup or it will be configured with a role other than the one you intended.
		/// </param>
		public BaseWorker(string workerName)
		{
			this.WorkerName = workerName;
		}

		#endregion

		#region PUBLIC METHODS

		public override bool OnStart()
		{
			// Set the maximum number of concurrent connections 
			ServicePointManager.DefaultConnectionLimit = 100;

			// Setup to handle service configuration changes at runtime.
			// For information on handling configuration changes
			// see the MSDN topic at http://go.microsoft.com/fwlink/?LinkId=166357.
			RoleEnvironment.Changing += this.RoleEnvironmentChanging;
			RoleEnvironment.Changed += this.RoleEnvironmentChanged;

			Trace.TraceInformation("'{0}' started", this.WorkerName);

			return base.OnStart();
		}

		public override void OnStop()
		{
			base.OnStop();
		}

		#endregion

		#region PRIVATE METHODS

		/// <summary>
		/// Event handler called when an environment change is to be applied to the role.
		/// Determines whether or not the role instance must be recycled.
		/// </summary>
		/// <param name="sender">The sender.</param>
		/// <param name="e">The list of changed environment values.</param>
		private void RoleEnvironmentChanging(object sender, RoleEnvironmentChangingEventArgs e)
		{
			// If Azure should recycle the role, e.Cancel should be set to true.
			// If the changes are ones we can handle without a recycle, we set it to false.
			e.Cancel = e.Changes.OfType<RoleEnvironmentConfigurationSettingChange>().Any();

			Trace.TraceInformation("'{0}' - Environment change - role instance recycling: {1}", this.WorkerName, e.Cancel);
		}

		/// <summary>
		/// Event handler called after an environment change has been applied to the role.
		/// </summary>
		/// <param name="sender">The sender.</param>
		/// <param name="e">The list of changed environment values.</param>
		private void RoleEnvironmentChanged(object sender, RoleEnvironmentChangedEventArgs e)
		{
		}

		#endregion
	}

}
