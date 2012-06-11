using System;
using System.IO;
using System.Security.Policy;
using System.Web;
using log4net;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	[Serializable]
	public class RabbitMqAddress
	{
		private readonly string broker;
		private readonly string exchange;
		private string queueName;
		private static readonly ILog log = LogManager.GetLogger(typeof(RabbitMqAddress));
		private string routingKeys;
		private bool routeByType;

		private const string ExchangeKey = "exchange";
		private const string QueueKey = "queue";
		private const string RoutingKeysKey = "routingKey";
		private const string RouteByTypeKey = "routeByType";

		public string Broker
		{
			get { return broker; }
		}

		public string Exchange
		{
			get { return exchange; }
		}

		public string QueueName
		{
			get { return queueName; }
		}

		public string RoutingKeys
		{
			get { return routingKeys; }
		}

		public bool RouteByType
		{
			get { return routeByType; }
		}

		public RabbitMqAddress(string broker, string exchange, string queueName)
			: this(broker, exchange, queueName, string.Empty, false) { }

		public RabbitMqAddress(string broker, string exchange, string queueName, string routingKeys)
			: this(broker, exchange, queueName, routingKeys, false) {}

		public RabbitMqAddress(string broker, string exchange, string queueName, string routingKeys, bool routeByType)
		{
			if(string.IsNullOrEmpty(exchange)
				&& (routeByType || string.IsNullOrEmpty(routingKeys) == false))
				throw new InvalidOperationException(
					"Cannot specify RouteByType or RoutingKeys and not specify an Exchange");

			if (string.IsNullOrEmpty(queueName) == false
				&& (routeByType || string.IsNullOrEmpty(routingKeys) == false))
			{
				log.Warn("Queue specified with Routing Keys, Routing Keys take precedence");
				//throw new InvalidOperationException(
				//    "Cannot specify destination Queue and (RouteByType or RoutingKeys)");
			}

			this.broker = broker.ToLower(); //always do this for good  measure (makes comparison easier)
			this.exchange = exchange;
			this.queueName = queueName;
			this.routingKeys = routingKeys;
			this.routeByType = routeByType;
		}

		public static RabbitMqAddress FromString(string value)
		{
			var uri = new Uri(value);
			var broker = uri.Host;

			if(uri.Port != -1)
				broker += ":" + uri.Port.ToString();

			var query = HttpUtility.ParseQueryString(uri.Query);

			var exchange = query[ExchangeKey] ?? string.Empty;
			var queue = query[QueueKey] ?? string.Empty;
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

			return new RabbitMqAddress(broker, exchange, queue, routingKeys, routeByType);
		}

		public string[] GetRoutingKeysAsArray()
		{
			if(string.IsNullOrEmpty(RoutingKeys))
				return new string[0];

			return RoutingKeys.Split(new [] {" "}, StringSplitOptions.RemoveEmptyEntries);
		}

		public string ToString(string route)
		{
			return (new RabbitMqAddress(Broker, Exchange, string.Empty, route)).ToString();
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
						+ HttpUtility.UrlEncode(key)
						+ "="
						+ HttpUtility.UrlEncode(value);
				};

			var uri = "rmq://" + broker + "/?";

			if (!string.IsNullOrEmpty(exchange))
				uri = addParam(uri, ExchangeKey, exchange);

			if (!string.IsNullOrEmpty(queueName))
				uri = addParam(uri, QueueKey, queueName);

			if (!string.IsNullOrEmpty(routingKeys))
				uri = addParam(uri, RoutingKeysKey, routingKeys);

			return uri;
		}

		public bool Equals(RabbitMqAddress other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Equals(other.broker, broker) && Equals(other.exchange, exchange) && Equals(other.queueName, queueName) && Equals(other.routingKeys, routingKeys) && other.routeByType.Equals(routeByType);
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
				int result = (broker != null ? broker.GetHashCode() : 0);
				result = (result * 397) ^ (exchange != null ? exchange.GetHashCode() : 0);
				result = (result * 397) ^ (queueName != null ? queueName.GetHashCode() : 0);
				result = (result * 397) ^ (routingKeys != null ? routingKeys.GetHashCode() : 0);
				result = (result * 397) ^ routeByType.GetHashCode();
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