using NServiceBus;

namespace MyMessages
{
	public abstract class LogMessage : IMessage
	{
		public string Message { get; set; }
	}
}

namespace Kernel
{
	public class Critical : MyMessages.LogMessage { }
	public class Error : MyMessages.LogMessage { }
	public class Warn : MyMessages.LogMessage {}
	public class Info : MyMessages.LogMessage {}

}

namespace Windows
{
	public class Critical : MyMessages.LogMessage { }
	public class Error : MyMessages.LogMessage { }
	public class Warn : MyMessages.LogMessage { }
	public class Info : MyMessages.LogMessage { }
}