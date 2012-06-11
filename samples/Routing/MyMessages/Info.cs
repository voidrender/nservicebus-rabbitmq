using NServiceBus;

namespace MyMessages
{
	public class Error: LogMessage {}
	public class Warn : LogMessage {}
	public class Info : LogMessage {}

	public abstract class LogMessage : IMessage
	{
		public string Message { get; set; }
	}
}