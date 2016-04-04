using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Picton.IntegrationTests
{
	[TestClass]
	public class AssemblyInitialize
	{
		[AssemblyInitialize]
		public static void AzureInitialize(TestContext testContext)
		{
			AzureStorageEmulatorManager.StartStorageEmulator();
		}
	}
}
