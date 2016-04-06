using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Picton.UnitTests
{
	[TestClass]
	public class AsyncMessagePumpTests
	{
		// The AsyncMessagePump constructor accepts a CloudQueue parameter
		// However, this class is sealed and cannot be mocked.
		// Therefore, unit testing is not really possible.
	}
}
