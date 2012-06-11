using NServiceBus;

namespace MyMessages
{
	public class SimpleMessage : IMessage
	{
		public string Message { get; set; }
	}
}