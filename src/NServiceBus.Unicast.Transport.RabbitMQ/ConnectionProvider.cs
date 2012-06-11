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

		private static readonly ConcurrentDictionary<string, IConnection> connections
			= new ConcurrentDictionary<string, IConnection>();
		
		[ThreadStatic]
		private static Dictionary<string, OpenedSession> state;
		

		public IModel Open(string protocolName, RabbitMqAddress brokerAddress, bool transactional)
		{
			if (!transactional)
			{
				var opened = OpenNew(protocolName, brokerAddress);
				//opened.Disposed += (sender, e) => log.Debug("Closing " + brokerAddress);
				return opened;
			}
			return OpenTransactional(protocolName, brokerAddress);
		}

		private IModel OpenTransactional(string protocolName, RabbitMqAddress brokerAddress)
		{
			//var key = string.Format("{0}:{1}", protocolName, brokerAddress);
			return OpenNew(protocolName, brokerAddress);
			//opened.Disposed += (sender, e) =>
			//                    {
			//                        log.Debug("Closing " + brokerAddress);
			//                        if (state != null)
			//                        {
			//                            var cnx = opened.Connection;
			//                            connections.TryRemove(key, out cnx);
			//                            state.Remove(brokerAddress.ToString());
			//                        }
			//                    };
			/*
		  if (Transaction.Current != null)
		  {
			Transaction.Current.EnlistVolatile(new RabbitMqEnlistment(opened), EnlistmentOptions.None);
			opened.AddRef();
		  }
		  
			return opened.AddRef();*/
		}

		private IModel OpenNew(string protocolName, RabbitMqAddress brokerAddress)
		{
			var protocol = GetProtocol(protocolName);
			var factory = GetConnectionFactory(protocol, brokerAddress);
			var connection = GetConnection(protocol, brokerAddress, factory);
			var model = connection.CreateModel();
			return model;
		}

		private ConnectionFactory GetConnectionFactory(IProtocol protocol, RabbitMqAddress brokerAddress)
		{
			ConnectionFactory factory = null;
			var key = string.Format("{0}:{1}", protocol, brokerAddress);
			
			if(!connectionFactories.TryGetValue(key, out factory))
			{
				factory = new ConnectionFactory();
				factory.Endpoint = new AmqpTcpEndpoint(protocol, brokerAddress.Broker);

				if(!string.IsNullOrEmpty(brokerAddress.VirtualHost))
					factory.VirtualHost = brokerAddress.VirtualHost;

				if(!string.IsNullOrEmpty(brokerAddress.Username))
					factory.UserName = brokerAddress.Username;

				if(!string.IsNullOrEmpty(brokerAddress.Password))
					factory.Password = brokerAddress.Password;

				factory = connectionFactories.GetOrAdd(key, factory);
				log.Debug("Opening new Connection Factory " + brokerAddress + " using " + protocol.ApiName);
			}

			return factory;
		}

		private IConnection GetConnection(IProtocol protocol, RabbitMqAddress brokerAddress, ConnectionFactory factory)
		{
			IConnection connection = null;
			var key = string.Format("{0}:{1}", protocol, brokerAddress);

			if (!connections.TryGetValue(key, out connection))
			{
				var newConnection = factory.CreateConnection();
				connection = connections.GetOrAdd(key, newConnection);

				//if someone else beat us from another thread kill the connection just created
				if(newConnection.Equals(connection) == false)
				{
					newConnection.Dispose();
				}
				else
				{
					log.DebugFormat("Opening new Connection {0} on {1} using {2}",
					                connection, brokerAddress, protocol.ApiName);
				}

			}

			return connection;
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