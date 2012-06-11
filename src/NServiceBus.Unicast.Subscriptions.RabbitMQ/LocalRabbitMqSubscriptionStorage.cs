using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using log4net;
using NServiceBus.Unicast.Subscriptions.RabbitMQ.Config;

namespace NServiceBus.Unicast.Subscriptions.RabbitMQ
{
	public class LocalRabbitMqSubscriptionStorage : ISubscriptionStorage
	{
		private readonly ILog log = LogManager.GetLogger(typeof(ISubscriptionStorage));

		private readonly Dictionary<string, List<string>> storage = new Dictionary<string, List<string>>();

		/// <summary>
		/// Subscribes the given client address to messages of the given types.
		/// </summary>
		/// <param name="client"/><param name="messageTypes"/>
		public void Subscribe(string client, IList<string> messageTypes)
		{
			var sectionName = typeof(LocalRabbitMqSubscriptionStorageConfig).Name;
			var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
			var section = config.GetSection(sectionName) as LocalRabbitMqSubscriptionStorageConfig;

			messageTypes.ToList().ForEach(m =>
			{
				if (!storage.ContainsKey(m))
					storage[m] = new List<string>();

				if (!storage[m].Contains(client))
					storage[m].Add(client);

				if (section == null)
					return;

				var subscribers = section.MessageEndpointMappings
					.Cast<MessageEndpointMapping>()
					.Where(x => x.Endpoint == client && x.Message == m)
					.ToArray();

				if (subscribers.Length > 0)
					return;

				section.MessageEndpointMappings
					.Add(
						new MessageEndpointMapping
							{
								Endpoint = client,
								Message = m
							});

				config.Save(ConfigurationSaveMode.Minimal);
				ConfigurationManager.RefreshSection(sectionName);
			});
		}

		public void SubscribeAndDontBotherWithConfig(string client, IList<string> messageTypes)
		{
			messageTypes.ToList().ForEach(m =>
			{
				if (!storage.ContainsKey(m))
					storage[m] = new List<string>();

				if (!storage[m].Contains(client))
					storage[m].Add(client);
			});
		}

		/// <summary>
		/// Unsubscribes the given client address from messages of the given types.
		/// </summary>
		/// <param name="client"/><param name="messageTypes"/>
		public void Unsubscribe(string client, IList<string> messageTypes)
		{
			var sectionName = typeof (LocalRabbitMqSubscriptionStorageConfig).Name;
			var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
			var section = config.GetSection(sectionName) as LocalRabbitMqSubscriptionStorageConfig;

			messageTypes.ToList().ForEach(m =>
			{
				if (storage.ContainsKey(m))
					storage[m].Remove(client);

				if (section == null)
					return;

				var subscribers = section.MessageEndpointMappings
					.Cast<MessageEndpointMapping>()
					.Where(x => x.Endpoint == client && x.Message == m)
					.ToArray();

				foreach(var subscriber in subscribers)
					section.MessageEndpointMappings.Remove(subscriber);

				if (subscribers.Length < 1) 
					return;

				config.Save(ConfigurationSaveMode.Minimal);
				ConfigurationManager.RefreshSection(sectionName);
			});
		}

		/// <summary>
		/// Returns a list of addresses of subscribers that previously requested to be notified
		///             of messages of the given message types.
		/// </summary>
		/// <param name="messageTypes"/>
		/// <returns/>
		public IList<string> GetSubscribersForMessage(IList<string> messageTypes)
		{
			var result = new List<string>();

			
			messageTypes.ToList().ForEach(m =>
			{
				if (storage.ContainsKey(m))
					result.AddRange(storage[m]);
			});

			return result;
		}

		/// <summary>
		/// Notifies the subscription storage that now is the time to perform
		///             any initialization work
		/// </summary>
		public void Init()
		{
			var sectionName = typeof (LocalRabbitMqSubscriptionStorageConfig).Name;
			var config = ConfigurationManager.OpenExeConfiguration(ConfigurationUserLevel.None);
			var section = config.GetSection(sectionName) as LocalRabbitMqSubscriptionStorageConfig;

			if(section == null)
			{
				log.Debug("No ConfigSection found: " + sectionName);
				return;
			}

			foreach(MessageEndpointMapping endpoint in section.MessageEndpointMappings)
			{
				var type = Type.GetType(endpoint.Message);

				if(type == null)
				{
					log.Warn("No type found: " + endpoint.Message);
					continue;
				}

				var message = type.AssemblyQualifiedName;
				SubscribeAndDontBotherWithConfig(endpoint.Endpoint, new[] { message });
			}
		}
	}
}
