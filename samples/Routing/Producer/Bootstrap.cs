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
				var read = Console.ReadLine();
				LogMessage message = null;
				read = string.IsNullOrEmpty(read) ? "info Info Message" : read;

				if (read.IndexOf(" ") < 0)
					read += " Log Message";

				var type = read.Substring(0, read.IndexOf(" "));
				var payload = read.Substring(read.IndexOf(" ") + 1);

				switch (type.ToLower())
				{
					case "warn":
						message = new Warn();
						break;
					case "error":
						message = new Error();
						break;
					default:
						message = new Info();
						break;
				}

				message.Message = payload;
				Bus.Send(message);
			}
		}

		public void Stop()
		{
		}
	}
}