using System;
using MyMessages;
using NServiceBus;

namespace Consumer2
{
	public class ErrorMessageHandler : IHandleMessages<LogMessage>
	{
		public void Handle(LogMessage message)
		{
			Console.WriteLine("---- Received Critical Log Message ----");
			Console.WriteLine("---- Type: " + message.GetType().FullName);
			Console.WriteLine("---- Message: ");
			Console.WriteLine(message.Message);
		}
	}
}