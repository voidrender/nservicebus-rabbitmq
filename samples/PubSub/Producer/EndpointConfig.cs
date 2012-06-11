using System;
using MyMessages;
using NServiceBus;
using NServiceBus.Unicast.Subscriptions.RabbitMQ.Config;
using NServiceBus.Unicast.Transport.RabbitMQ.Config;

namespace Producer
{
	public class EndpointConfig : IConfigureThisEndpoint, IWantCustomInitialization
	{
		public void Init()
		{
			Configure.With()
				.DefaultBuilder()
				.XmlSerializer()
				.RabbitMqTransport()
				.LocalRabbitMqSubcriptionStorage()
				.UnicastBus();
		}
	}
}