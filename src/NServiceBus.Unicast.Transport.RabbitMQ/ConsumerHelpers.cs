using System;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	public static class ConsumerHelpers
	{
		private static readonly DateTime UnixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, 0);

		public static BasicDeliverEventArgs Receive(this QueueingBasicConsumer consumer, TimeSpan to)
		{
			object delivery;
			if (!consumer.Queue.Dequeue((Int32) to.TotalMilliseconds, out delivery))
			{
				return null;
			}
			return delivery as BasicDeliverEventArgs;
		}

		public static DateTime ToDateTime(this AmqpTimestamp timestamp)
		{
			return UnixEpoch.AddSeconds(timestamp.UnixTime);
		}

		public static AmqpTimestamp ToAmqpTimestamp(this DateTime dateTime)
		{
			return new AmqpTimestamp((long) (dateTime - UnixEpoch).TotalSeconds);
		}
	}
}