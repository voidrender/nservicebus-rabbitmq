using System;
using MyMessages;
using NServiceBus;

namespace Producer
{
	public class Bootstrap : IWantToRunAtStartup
	{
		public IBus Bus { get; set; }

		public void Run()
		{
			Console.WriteLine("Press 'Enter' to send a message. To exit, Ctrl + C");

			while (true)
			{
				var payload = Console.ReadLine();
				payload = string.IsNullOrEmpty(payload) ? "Hello World" : payload;
				Console.WriteLine("Sending Message: {0}", payload);
				Bus.Publish<SimpleMessage>( m => { m.Message = payload; });
			}
		}

		public void Stop()
		{
		}
	}
}