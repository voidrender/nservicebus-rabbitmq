using System;
using MyMessages;
using NServiceBus;

namespace Consumer1
{
	public class LogMessageHandler : IHandleMessages<LogMessage>
	{
		public void Handle(LogMessage message)
		{
			Console.WriteLine("---- Received Kernel Log Message ----");
			Console.WriteLine("---- Type: ");
			Console.WriteLine(message.GetType().FullName);
			Console.WriteLine("---- Message: ");
			Console.WriteLine(message.Message);
		}
	}
}