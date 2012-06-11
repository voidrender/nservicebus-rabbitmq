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
				read = string.IsNullOrEmpty(read) ? "kernel info Info Message" : read;

				string type;
				string payload;
				string level;

				try
				{
					type = read.Substring(0, read.IndexOf(" "));
					read = read.Substring(read.IndexOf(" ") + 1);
					level = read.Substring(0, read.IndexOf(" "));
					payload = read.Substring(read.IndexOf(" ") + 1);
				}
				catch
				{
					Console.WriteLine("Enter of format: {system} {level} {message}");
					continue;
				}

				if(type.ToLower() == "kernel")
				{
					switch (level.ToLower())
					{
						case "warn":
							message = new Kernel.Warn();
							break;
						case "error":
							message = new Kernel.Error();
							break;
						case "critical":
							message = new Kernel.Critical();
							break;
						default:
							message = new Kernel.Info();
							break;
					}
				}
				else if(type.ToLower() == "windows")
				{
					switch (level.ToLower())
					{
						case "warn":
							message = new Windows.Warn();
							break;
						case "error":
							message = new Windows.Error();
							break;
						case "critical":
							message = new Windows.Critical();
							break;
						default:
							message = new Windows.Info();
							break;
					}
				}
				else
				{
					Console.WriteLine("Unknown system: " + type);
					continue;
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