using System;
using MyMessages;
using NServiceBus;

namespace Consumer2
{
	public class ErrorMessageHandler : IHandleMessages<Error>
	{
		public void Handle(Error message)
		{
			Console.WriteLine("---- Received Error Log Message ----");
			Console.WriteLine("---- Message: ");
			Console.WriteLine(message.Message);
		}
	}
}