using System;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Security.Policy;
using System.Text;
using System.Web;
using log4net;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	[Serializable]
	public class RabbitMqAddress
	{
		private static readonly ILog log = LogManager.GetLogger(typeof(RabbitMqAddress));

		private const string VirtualHostKey = "virtualHost";
		private const string UsernameKey = "username";
		private const string PasswordKey = "password";
		private const string ExchangeKey = "exchange";
		private const string QueueKey = "queue";
		private const string RoutingKeysKey = "routingKey";
		private const string RouteByTypeKey = "routeByType";

		public string Broker { get; private set; }

		public string Username { get; private set; }

		public string Password { get; private set; }

		public string VirtualHost { get; private set; }

		public string Exchange { get; private set; }

		public string QueueName { get; private set; }

		public string RoutingKeys { get; private set; }

		public bool RouteByType { get; private set; }

		public RabbitMqAddress(string broker, string vhost, string username, string password, string exchange, string queueName, string routingKeys, bool routeByType)
		{
			if(string.IsNullOrEmpty(exchange)
				&& (routeByType || string.IsNullOrEmpty(routingKeys) == false))
				throw new InvalidOperationException(
					"Cannot specify RouteByType or RoutingKeys and not specify an Exchange");

			if (string.IsNullOrEmpty(queueName) == false
				&& (routeByType || string.IsNullOrEmpty(routingKeys) == false))
			{
				log.Warn("Queue specified with Routing Keys, Routing Keys take precedence");
			}

			Broker = broker.ToLower(); //always do this for good  measure (makes comparison easier)
			VirtualHost = vhost;
			Username = username;
			Password = password;
			Exchange = exchange;
			QueueName = queueName;
			RoutingKeys = routingKeys;
			RouteByType = routeByType;

			log.Info("Broker:" + broker);
			log.Info("Exchange:" + exchange);
			log.Info("QueueName:" + queueName);
			log.Info("RoutingKeys:" + routingKeys);
			log.Info("RouteByType:" + routeByType);
		}

		public static RabbitMqAddress FromString(string value)
		{
			var uri = new Uri(value);
			var broker = uri.Host;

			if(uri.Port != -1)
				broker += ":" + uri.Port.ToString();

			var query = new NameValueCollection();

			var queryString = uri.Query;
			if (queryString.StartsWith("?"))
				queryString = queryString.Substring(1);

			queryString.Split(new[] { "&" }, StringSplitOptions.RemoveEmptyEntries)
				.ToList()
				.ForEach(
					x =>
				        {
				         	var parts = x.Split(new[] {"="}, StringSplitOptions.RemoveEmptyEntries);
				         	string key = string.Empty;
				         	string val = string.Empty;
							if (parts.Length > 0)
								key = parts[0];
							if (parts.Length > 1)
								val = Uri.UnescapeDataString(parts[1]);

							query.Add(key, val);
				        });

			var exchange = query[ExchangeKey] ?? string.Empty;
			var queue = query[QueueKey] ?? string.Empty;
			var vhost = query[VirtualHostKey] ?? string.Empty;
			var username = query[UsernameKey] ?? string.Empty;
			var password = query[PasswordKey] ?? string.Empty;
			var routingKeys = query[RoutingKeysKey] ?? string.Empty;
			var routeByTypeValue = query[RouteByTypeKey];

			var routeByType = 
				string.IsNullOrEmpty(routeByTypeValue) 
					? false 
					: bool.Parse(routeByTypeValue);

			if(string.IsNullOrEmpty(exchange) 
				&& string.IsNullOrEmpty(queue)
				&& string.IsNullOrEmpty(routingKeys))
			{
				var message = "No Exchange, Queue, or RoutingKeys defined for endpoint: " + value;
				log.Error(message);
				throw new InvalidOperationException(message);
			}

			return new RabbitMqAddress(broker, vhost, username, password, exchange, queue, routingKeys, routeByType);
		}

		public string[] GetRoutingKeysAsArray()
		{
			if(string.IsNullOrEmpty(RoutingKeys))
				return new string[0];

			return RoutingKeys.Split(new [] {" "}, StringSplitOptions.RemoveEmptyEntries);
		}

		public string ToString(string route)
		{
			return (new RabbitMqAddress(Broker, VirtualHost, Username, Password, Exchange, string.Empty, route, false)).ToString();
		}

		public override string ToString()
		{
			Func<string, string, string, string> addParam =
				(source, key, value) =>
				{
					string delim;
					if (string.IsNullOrEmpty(source) || !source.Contains("?"))
						delim = "?";
					else if (source.EndsWith("?") || source.EndsWith("&"))
						delim = string.Empty;
					else
						delim = "&";

					return source
						+ delim
						+ Uri.EscapeDataString(key)
						+ "="
						+ Uri.EscapeDataString(value);
				};

			var uri = "rmq://" + Broker + "/?";

			if (!string.IsNullOrEmpty(VirtualHost))
				uri = addParam(uri, VirtualHostKey, VirtualHost);

			if (!string.IsNullOrEmpty(Username))
				uri = addParam(uri, UsernameKey, Username);

			if (!string.IsNullOrEmpty(Password))
				uri = addParam(uri, PasswordKey, Password);

			if (!string.IsNullOrEmpty(Exchange))
				uri = addParam(uri, ExchangeKey, Exchange);

			if (!string.IsNullOrEmpty(QueueName))
				uri = addParam(uri, QueueKey, QueueName);

			if (!string.IsNullOrEmpty(RoutingKeys))
				uri = addParam(uri, RoutingKeysKey, RoutingKeys);

			return uri;
		}

		public bool Equals(RabbitMqAddress other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.Broker, Broker) && Equals(other.Exchange, Exchange) && Equals(other.QueueName, QueueName) && Equals(other.RoutingKeys, RoutingKeys) && other.RouteByType.Equals(RouteByType);
		}

		public override bool Equals(object obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != typeof(RabbitMqAddress)) return false;
			return Equals((RabbitMqAddress)obj);
		}

		public override int GetHashCode()
		{
			unchecked
			{
				int result = (Broker != null ? Broker.GetHashCode() : 0);
				result = (result * 397) ^ (VirtualHost != null ? VirtualHost.GetHashCode() : 0);
				result = (result * 397) ^ (Username != null ? Username.GetHashCode() : 0);
				result = (result * 397) ^ (Password != null ? Password.GetHashCode() : 0);
				result = (result * 397) ^ (Exchange != null ? Exchange.GetHashCode() : 0);
				result = (result * 397) ^ (QueueName != null ? QueueName.GetHashCode() : 0);
				result = (result * 397) ^ (RoutingKeys != null ? RoutingKeys.GetHashCode() : 0);
				result = (result * 397) ^ RouteByType.GetHashCode();
				return result;
			}
		}

		public static bool operator ==(RabbitMqAddress left, RabbitMqAddress right)
		{
			return Equals(left, right);
		}

		public static bool operator !=(RabbitMqAddress left, RabbitMqAddress right)
		{
			return !Equals(left, right);
		}

	}
}