using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using log4net;
using RabbitMQ.Client;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	public class ConnectionProvider
	{
		private readonly ILog log = LogManager.GetLogger(typeof (ConnectionProvider));

		private static readonly ConcurrentDictionary<string, ConnectionFactory> connectionFactories
			= new ConcurrentDictionary<string, ConnectionFactory>();

		[ThreadStatic] private static Dictionary<string, OpenedSession> state;

		public OpenedSession Open(string protocolName, string brokerAddress, bool transactional)
		{
			if (!transactional)
			{
				var opened = OpenNew(protocolName, brokerAddress);
				opened.Disposed += (sender, e) => log.Debug("Closing " + brokerAddress);
				return opened;
			}
			return OpenTransactional(protocolName, brokerAddress);
		}

		private OpenedSession OpenTransactional(string protocolName, string brokerAddress)
		{
			if (state == null)
			{
				state = new Dictionary<string, OpenedSession>();
			}
			if (state.ContainsKey(brokerAddress))
			{
				var existing = state[brokerAddress].AddRef();
				if (existing.IsActive)
				{
					return existing;
				}
				state.Remove(brokerAddress);
			}
			var opened = state[brokerAddress] = OpenNew(protocolName, brokerAddress);
			opened.Disposed += (sender, e) =>
			                   	{
			                   		log.Debug("Closing " + brokerAddress);
			                   		if (state != null)
			                   		{
			                   			state.Remove(brokerAddress);
			                   		}
			                   	};
			/*
		  if (Transaction.Current != null)
		  {
			Transaction.Current.EnlistVolatile(new RabbitMqEnlistment(opened), EnlistmentOptions.None);
			opened.AddRef();
		  }
		  */
			return opened.AddRef();
		}

		private OpenedSession OpenNew(string protocolName, string brokerAddress)
		{
			var protocol = GetProtocol(protocolName);
			var factory = GetConnectionFactory(protocol, brokerAddress);
			var connection = factory.CreateConnection();//protocol, brokerAddress);
			var model = connection.CreateModel();
			log.Debug("Opening " + brokerAddress + " using " + protocol.ApiName);
			return new OpenedSession(connection, model);
		}

		private ConnectionFactory GetConnectionFactory(IProtocol protocol, string brokerAddress)
		{
			ConnectionFactory factory = null;
			var key = string.Format("{0}:{1}", protocol, brokerAddress);
			
			if(!connectionFactories.TryGetValue(key, out factory))
			{
				factory = new ConnectionFactory();
				factory.Endpoint = new AmqpTcpEndpoint(protocol, brokerAddress);
				factory = connectionFactories.GetOrAdd(key, factory);
			}

			return factory;
		}

		private static IProtocol GetProtocol(string protocolName)
		{
			if (String.IsNullOrEmpty(protocolName))
			{
				return Protocols.FromConfiguration();
			}
			return Protocols.SafeLookup(protocolName);
		}
	}
}