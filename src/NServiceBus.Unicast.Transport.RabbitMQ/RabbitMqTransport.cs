using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Transactions;
using log4net;
using NServiceBus.Serialization;
using NServiceBus.Unicast.Transport.Msmq;
using NServiceBus.Utils;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	public class RabbitMqTransport : ITransport
	{
		private readonly ConnectionProvider connectionProvider = new ConnectionProvider();
		private readonly ILog log = LogManager.GetLogger(typeof (RabbitMqTransport));
		private readonly List<WorkerThread> workers = new List<WorkerThread>();
		private readonly ReaderWriterLockSlim failuresPerMessageLocker 
			= new ReaderWriterLockSlim();
		private readonly IDictionary<string, Int32> failuresPerMessage 
			= new Dictionary<string, Int32>();
		private RabbitMqAddress inputAddress;
		private RabbitMqAddress errorAddress;
		private Int32 numberOfWorkerThreads;
		private Int32 maximumNumberOfRetries;
		private IsolationLevel isolationLevel;
		private TimeSpan transactionTimeout = TimeSpan.FromMinutes(5);
		private TimeSpan receiveTimeout = TimeSpan.FromSeconds(1);

		private class MessageReceiveProperties
		{
			public string MessageId { get; set; }
			public bool NeedToAbort { get; set; }
		}

		[ThreadStatic] private static MessageReceiveProperties messageContext;

		public void Start()
		{
			CreateExchangesAndQueuesIfNecessary();
			BindExchangesAndQueues();

			inputAddress = new RabbitMqAddress(InputBroker, InputExchange, InputQueue, InputRoutingKeys);
			errorAddress = new RabbitMqAddress(ErrorBroker, ErrorExchange, ErrorQueue, ErrorRoutingKeys);

			for (var i = 0; i < numberOfWorkerThreads; ++i)
				AddWorkerThread().Start();
		}

		public void ChangeNumberOfWorkerThreads(Int32 targetNumberOfWorkerThreads)
		{
			lock (workers)
			{
				var numberOfThreads = workers.Count;
				if (targetNumberOfWorkerThreads == numberOfThreads) return;
				if (targetNumberOfWorkerThreads < numberOfThreads)
				{
					for (var i = targetNumberOfWorkerThreads; i < numberOfThreads; i++)
					{
						workers[i].Stop();
					}
				}
				else if (targetNumberOfWorkerThreads > numberOfThreads)
				{
					for (var i = numberOfThreads; i < targetNumberOfWorkerThreads; i++)
					{
						AddWorkerThread().Start();
					}
				}
			}
		}

		private void CreateExchangesAndQueuesIfNecessary()
		{
			if (DoNotCreateInputExchange == false)
				DeclareExchange(InputBroker, InputExchange, InputExchangeType);

			if (DoNotCreateInputQueue == false)
				DeclareQueue(InputBroker, InputQueue);

			if (DoNotCreateErrorExchange == false)
				DeclareExchange(ErrorBroker, ErrorExchange, ErrorExchangeType);

			if (DoNotCreateErrorQueue == false)
				DeclareQueue(ErrorBroker, ErrorQueue);
		}

		private void DeclareQueue(string broker, string queue)
		{
			if (string.IsNullOrEmpty(queue))
			{
				log.Info("No Queue Provided. Not attempting Declare");
				return;
				//var message = "No Input Queue Provided. Cannot Declare Queue";
				//log.Error(message);
				//throw new InvalidOperationException(message);
			}

			using (var connection = connectionProvider.Open(ProtocolName, broker, true))
			{
				var channel = connection.Model();
				log.InfoFormat("Declaring Queue {0} on Broker {1}", queue, broker);
				channel.QueueDeclare(queue, false, false, false, null);
			}
		}

		private void DeclareExchange(string broker, string exchange, string exchangeType)
		{
			if (string.IsNullOrEmpty(exchange))
			{
				log.Info("No Exchange Provided. Not attempting Declare");
				return;
				//var message = "No Input Exchange Provided. Cannot Declare Exchange";
				//log.Error(message);
				//throw new InvalidOperationException(message);
			}

			using (var connection = connectionProvider.Open(ProtocolName, broker, true))
			{
				var channel = connection.Model();

				log.InfoFormat(
					"Declaring Exchange {0} of Type {1} on Broker {2}",
					exchange,
					exchangeType,
					broker);

				channel.ExchangeDeclare(exchange, exchangeType);
			}
		}

		private void BindExchangesAndQueues()
		{
			BindQueue(InputBroker, InputExchange, InputQueue, InputRoutingKeys);
			BindQueue(ErrorBroker, ErrorExchange, ErrorQueue, ErrorRoutingKeys);
		}

		private void BindQueue(string broker, string exchange, string queue, string routingKeys)
		{
			if(string.IsNullOrEmpty(exchange))
				return;

			using (var connection = connectionProvider.Open(ProtocolName, broker, true))
			{
				var channel = connection.Model();
				var keys = routingKeys.Split(new[] { " " }, StringSplitOptions.RemoveEmptyEntries);
				keys = keys.Length == 0 ? new[] {""} : keys;

				foreach(var key in keys)
				{
					log.InfoFormat("Binding Key {0} on Queue {1} on Exchange {2}", key, queue, exchange);
					channel.QueueBind(queue, exchange, key);
				}
			}
		}

		public void Send(TransportMessage transportMessage, string destination)
		{
			var address = RabbitMqAddress.FromString(destination);

			//if sending locally we need to munge some stuff (remove routing keys)
			if(address == inputAddress)
				address = 
					new RabbitMqAddress(
						inputAddress.Broker, inputAddress.Exchange, inputAddress.QueueName);

			using (var stream = new MemoryStream())
			{
				this.MessageSerializer.Serialize(transportMessage.Body, stream);
				using (var connection = connectionProvider.Open(this.ProtocolName, address.Broker, true))
				{
					var channel = connection.Model();
					var messageId = Guid.NewGuid().ToString();
					var properties = channel.CreateBasicProperties();
					properties.MessageId = messageId;
					if (!String.IsNullOrEmpty(transportMessage.CorrelationId))
					{
						properties.CorrelationId = transportMessage.CorrelationId;
					}

					properties.Timestamp = DateTime.UtcNow.ToAmqpTimestamp();
					properties.ReplyTo = this.InputAddress;
					properties.SetPersistent(transportMessage.Recoverable);
					var headers = transportMessage.Headers;
					if (headers != null && headers.Count > 0)
					{
						var dictionary = headers
							.ToDictionary<HeaderInfo, string, object>
								(entry => entry.Key, entry => entry.Value);

						properties.Headers = dictionary;
					}

					if(address.RouteByType)
					{
						var type = transportMessage.Body[0].GetType();
						string typeName = type.FullName;
						log.InfoFormat("Sending message (routed) " + address.ToString(typeName) + " of " + type.Name);
						channel.BasicPublish(address.Exchange, typeName, properties, stream.ToArray());
						return;
					}

					var routingKeys = address.GetRoutingKeysAsArray();

					if(routingKeys.Length > 1)
					{
						var message = "Too many Routing Keys specified for endpoint: " + address.ToString();
						message += Environment.NewLine + "Keys: " + address.RoutingKeys;
						log.Error(message);
						throw new InvalidOperationException(message);
					}
					
					if(routingKeys.Length > 0)
					{
						var type = transportMessage.Body[0].GetType();
						log.InfoFormat("Sending message (routed) " + address.ToString() + " of " + type.Name);
						channel.BasicPublish(address.Exchange, routingKeys[0], properties, stream.ToArray());
						return;
					}

					log.Info("Sending message " + address.ToString() + " of " + transportMessage.Body[0].GetType().Name);
					channel.BasicPublish(address.Exchange, address.QueueName, properties, stream.ToArray());
					transportMessage.Id = properties.MessageId;
				}
			}
		}

		public void ReceiveMessageLater(TransportMessage transportMessage)
		{
			if (!String.IsNullOrEmpty(this.InputAddress))
			{
				Send(transportMessage, this.InputAddress);
			}
		}

		public Int32 GetNumberOfPendingMessages()
		{
			return 0;
		}

		public void AbortHandlingCurrentMessage()
		{
			if (messageContext != null)
			{
				messageContext.NeedToAbort = true;
			}
		}

		private WorkerThread AddWorkerThread()
		{
			lock (workers)
			{
				var newWorker = new WorkerThread(Process);
				workers.Add(newWorker);
				newWorker.Stopped +=
					((sender, e) =>
					{
						var worker = sender as WorkerThread;
						lock (workers)
						{
							log.Info("Removing Worker");
							workers.Remove(worker);
						}
					});
				return newWorker;
			}
		}

		private void Process()
		{
			messageContext = new MessageReceiveProperties();
			try
			{
				var wrapper = new TransactionWrapper();
				wrapper.RunInTransaction(() => Receive(messageContext), isolationLevel, transactionTimeout);
				ClearFailuresForMessage(messageContext.MessageId);
			}
			catch (AbortHandlingCurrentMessageException)
			{
			}
			catch (Exception error)
			{
				log.Error(error);
				IncrementFailuresForMessage(messageContext.MessageId);
				OnFailedMessageProcessing(error);
			}
			finally
			{
				messageContext = null;
			}
		}

		private void Receive(MessageReceiveProperties messageContext)
		{
			using (var connection = connectionProvider.Open(this.ProtocolName, inputAddress.Broker, true))
			{
				var channel = connection.Model();
				var consumer = new QueueingBasicConsumer(channel);

				channel.BasicConsume(inputAddress.QueueName, SendAcknowledgement, consumer);

				var delivery = consumer.Receive(receiveTimeout);
				if (delivery != null)
				{
					log.Debug("Receiving from " + inputAddress);
					DeliverMessage(channel, messageContext, delivery);
				}
			}
		}

		private void DeliverMessage(IModel channel, MessageReceiveProperties messageContext, BasicDeliverEventArgs delivery)
		{
			messageContext.MessageId = delivery.BasicProperties.MessageId;
			if (HandledMaximumRetries(messageContext.MessageId))
			{
				MoveToPoison(delivery);
				channel.BasicAck(delivery.DeliveryTag, false);
				return;
			}

			var startedProcessingError = OnStartedMessageProcessing();
			if (startedProcessingError != null)
			{
				throw new MessageHandlingException("Exception occured while starting to process message.", startedProcessingError,
												   null, null);
			}

			var m = new TransportMessage();
			try
			{
				using (var stream = new MemoryStream(delivery.Body))
				{
					m.Body = this.MessageSerializer.Deserialize(stream);
				}
			}
			catch (Exception deserializeError)
			{
				log.Error("Could not extract message data.", deserializeError);
				MoveToPoison(delivery);
				OnFinishedMessageProcessing();
				return;
			}
			m.Id = delivery.BasicProperties.MessageId;
			m.CorrelationId = delivery.BasicProperties.CorrelationId;
			m.IdForCorrelation = delivery.BasicProperties.MessageId;
			m.ReturnAddress = delivery.BasicProperties.ReplyTo;
			m.TimeSent = delivery.BasicProperties.Timestamp.ToDateTime();
			m.Headers = m.Headers ?? new List<HeaderInfo>();
			if (delivery.BasicProperties.Headers != null && delivery.BasicProperties.Headers.Count > 0)
			{
				foreach (DictionaryEntry entry in delivery.BasicProperties.Headers)
				{
					var value = entry.Value;
					var valueString = value != null ? value.ToString() : null;

					m.Headers.Add(
						new HeaderInfo
						{
							Key = entry.Key.ToString(),
							Value = valueString
						});
				}
			}

			m.Recoverable = delivery.BasicProperties.DeliveryMode == 2;
			var receivingError = OnTransportMessageReceived(m);
			var finishedProcessingError = OnFinishedMessageProcessing();
			if (messageContext.NeedToAbort)
			{
				throw new AbortHandlingCurrentMessageException();
			}
			if (receivingError != null || finishedProcessingError != null)
			{
				throw new MessageHandlingException("Exception occured while processing message.", null, receivingError,
												   finishedProcessingError);
			}
			channel.BasicAck(delivery.DeliveryTag, false);
		}

		private void IncrementFailuresForMessage(string id)
		{
			if (String.IsNullOrEmpty(id)) return;
			failuresPerMessageLocker.EnterWriteLock();
			try
			{
				if (!failuresPerMessage.ContainsKey(id))
				{
					failuresPerMessage[id] = 1;
				}
				else
				{
					failuresPerMessage[id] += 1;
				}
			}
			finally
			{
				failuresPerMessageLocker.ExitWriteLock();
			}
		}

		private void ClearFailuresForMessage(string id)
		{
			if (String.IsNullOrEmpty(id)) return;
			failuresPerMessageLocker.EnterReadLock();
			if (failuresPerMessage.ContainsKey(id))
			{
				failuresPerMessageLocker.ExitReadLock();
				failuresPerMessageLocker.EnterWriteLock();
				failuresPerMessage.Remove(id);
				failuresPerMessageLocker.ExitWriteLock();
			}
			else
			{
				failuresPerMessageLocker.ExitReadLock();
			}
		}

		private bool HandledMaximumRetries(string id)
		{
			if (String.IsNullOrEmpty(id)) return false;
			failuresPerMessageLocker.EnterReadLock();
			if (failuresPerMessage.ContainsKey(id) && (failuresPerMessage[id] == maximumNumberOfRetries))
			{
				failuresPerMessageLocker.ExitReadLock();
				failuresPerMessageLocker.EnterWriteLock();
				failuresPerMessage.Remove(id);
				failuresPerMessageLocker.ExitWriteLock();
				return true;
			}
			failuresPerMessageLocker.ExitReadLock();
			return false;
		}

		private void MoveToPoison(BasicDeliverEventArgs delivery)
		{
			if (errorAddress == null)
			{
				log.Info("Discarding " + delivery.BasicProperties.MessageId);
				return;
			}
			using (var connection = connectionProvider.Open(this.ProtocolName, inputAddress.Broker, false))
			{
				var channel = connection.Model();
				log.Info("Moving " + delivery.BasicProperties.MessageId + " to " + errorAddress);
				channel.BasicPublish(errorAddress.Exchange, errorAddress.QueueName, delivery.BasicProperties, delivery.Body);
			}
		}

		private Exception OnFailedMessageProcessing(Exception error)
		{
			try
			{
				if (this.FailedMessageProcessing != null)
				{
					this.FailedMessageProcessing(this, new ThreadExceptionEventArgs(error));
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'failed message processing' event.", processingError);
				return processingError;
			}
			return null;
		}

		private Exception OnStartedMessageProcessing()
		{
			try
			{
				if (this.StartedMessageProcessing != null)
				{
					this.StartedMessageProcessing(this, null);
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'started message processing' event.", processingError);
				return processingError;
			}
			return null;
		}

		private Exception OnFinishedMessageProcessing()
		{
			try
			{
				if (this.FinishedMessageProcessing != null)
				{
					this.FinishedMessageProcessing(this, null);
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'finished message processing' event.", processingError);
				return processingError;
			}
			return null;
		}

		private Exception OnTransportMessageReceived(TransportMessage msg)
		{
			try
			{
				if (this.TransportMessageReceived != null)
				{
					this.TransportMessageReceived(this, new TransportMessageReceivedEventArgs(msg));
				}
			}
			catch (Exception processingError)
			{
				log.Error("Failed raising 'transport message received' event.", processingError);
				return processingError;
			}
			return null;
		}

		public void Dispose()
		{
			lock (workers)
			{
				foreach (var worker in workers)
				{
					worker.Stop();
				}
			}
		}

		public event EventHandler<TransportMessageReceivedEventArgs> TransportMessageReceived;
		public event EventHandler StartedMessageProcessing;
		public event EventHandler FinishedMessageProcessing;
		public event EventHandler FailedMessageProcessing;

		public Int32 NumberOfWorkerThreads
		{
			get
			{
				lock (workers)
				{
					return workers.Count;
				}
			}
			set { numberOfWorkerThreads = value; }
		}

		public Int32 MaximumNumberOfRetries
		{
			get { return maximumNumberOfRetries; }
			set { maximumNumberOfRetries = value; }
		}

		public string Address
		{
			get
			{
				if (inputAddress == null)
					return null;
				return inputAddress.ToString();
			}
		}

		public string InputAddress
		{
			get { return inputAddress.ToString(); }
		}

		public string ErrorAddress
		{
			get { return errorAddress.ToString(); }
		}

		public IMessageSerializer MessageSerializer { get; set; }

		public IsolationLevel IsolationLevel
		{
			get { return isolationLevel; }
			set { isolationLevel = value; }
		}

		public string ProtocolName { get; set; }

		public TimeSpan TransactionTimeout
		{
			get { return transactionTimeout; }
			set { transactionTimeout = value; }
		}

		public TimeSpan ReceiveTimeout
		{
			get { return receiveTimeout; }
			set { receiveTimeout = value; }
		}

		public string InputExchangeType { get; set; }

		public bool SendAcknowledgement { get; set; }

		public string InputBroker { get; set; }

		public string InputExchange { get; set; }

		public string InputQueue { get; set; }

		public string InputRoutingKeys { get; set; }

		public bool DoNotCreateInputExchange { get; set; }

		public bool DoNotCreateInputQueue { get; set; }

		public string ErrorBroker { get; set; }

		public string ErrorExchange { get; set; }

		public string ErrorQueue { get; set; }

		public string ErrorRoutingKeys { get; set; }

		public string ErrorExchangeType { get; set; }

		public bool DoNotCreateErrorExchange { get; set; }

		public bool DoNotCreateErrorQueue { get; set; }
	}

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