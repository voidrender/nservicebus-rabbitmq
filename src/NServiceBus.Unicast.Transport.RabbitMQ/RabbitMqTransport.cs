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
			inputAddress = new RabbitMqAddress(InputBroker, InputVirtualHost, InputUsername, InputPassword, InputExchange, InputQueue, InputRoutingKeys, false);
			errorAddress = new RabbitMqAddress(ErrorBroker, ErrorVirtualHost, ErrorUsername, ErrorPassword, ErrorExchange, ErrorQueue, ErrorRoutingKeys, false);

			CreateExchangesAndQueuesIfNecessary();
			BindExchangesAndQueues();

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
				DeclareExchange(inputAddress, InputExchange, InputExchangeType);

			if (DoNotCreateInputQueue == false)
				DeclareQueue(inputAddress, InputQueue, InputIsDurable);

			if (DoNotCreateErrorExchange == false)
				DeclareExchange(errorAddress, ErrorExchange, ErrorExchangeType);

			if (DoNotCreateErrorQueue == false)
				DeclareQueue(errorAddress, ErrorQueue, ErrorIsDurable);
		}

		public bool ErrorIsDurable { get; set; }

		public bool InputIsDurable { get; set; }

		private void DeclareQueue(RabbitMqAddress broker, string queue, bool durable)
		{
			if (string.IsNullOrEmpty(queue))
			{
				log.Info("No Queue Provided. Not attempting Declare");
				return;
				//var message = "No Input Queue Provided. Cannot Declare Queue";
				//log.Error(message);
				//throw new InvalidOperationException(message);
			}

			using (var channel = connectionProvider.Open(ProtocolName, broker, true))
			{
				log.InfoFormat("Declaring Queue {0} on Broker {1}", queue, broker);
				channel.QueueDeclare(queue, durable, false, false, null);
			}
		}

		private void DeclareExchange(RabbitMqAddress broker, string exchange, string exchangeType)
		{
			if (string.IsNullOrEmpty(exchange))
			{
				log.Info("No Exchange Provided. Not attempting Declare");
				return;
				//var message = "No Input Exchange Provided. Cannot Declare Exchange";
				//log.Error(message);
				//throw new InvalidOperationException(message);
			}

			using (var channel = connectionProvider.Open(ProtocolName, broker, true))
			{
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
			BindQueue(inputAddress, InputExchange, InputQueue, InputRoutingKeys);
			BindQueue(errorAddress, ErrorExchange, ErrorQueue, ErrorRoutingKeys);
		}

		private void BindQueue(RabbitMqAddress broker, string exchange, string queue, string routingKeys)
		{
			if (string.IsNullOrEmpty(exchange))
				return;

			using (var channel = connectionProvider.Open(ProtocolName, broker, true))
			{
				var keys = routingKeys.Split(new[] { " " }, StringSplitOptions.RemoveEmptyEntries);
				keys = keys.Length == 0 ? new[] { queue } : keys;

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
						inputAddress.Broker, 
						inputAddress.VirtualHost, 
						inputAddress.Username, 
						inputAddress.Password, 
						inputAddress.Exchange, 
						inputAddress.QueueName, 
						string.Empty, 
						false);

			using (var stream = new MemoryStream())
			{
				this.MessageSerializer.Serialize(transportMessage.Body, stream);
				using (var channel = connectionProvider.Open(this.ProtocolName, address, true))
				{
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
			var wrapper = new TransactionWrapper();
			wrapper.RunInTransaction(() => Receive(messageContext), isolationLevel, transactionTimeout);
			ClearFailuresForMessage(messageContext.MessageId);
			messageContext = null;
		}

		private void Receive(MessageReceiveProperties messageContext)
		{
			using (var channel = connectionProvider.Open(this.ProtocolName, inputAddress, true))
			{
				var consumer = new QueueingBasicConsumer(channel);

				channel.BasicConsume(inputAddress.QueueName, false, consumer);

				var delivery = consumer.Receive(receiveTimeout);
				if (delivery != null)
				{
					try
					{
						log.Debug("Receiving from " + inputAddress);
						DeliverMessage(channel, messageContext, delivery);
					}
					catch (AbortHandlingCurrentMessageException)
					{
						return;
					}
					catch
					{
						//IncrementFailuresForMessage(messageContext.MessageId);
						MoveToPoison(delivery);
						OnFailedMessageProcessing();
						channel.BasicAck(delivery.DeliveryTag, false);
					}
				}
			}
		}

		private void DeliverMessage(IModel channel, MessageReceiveProperties messageContext, BasicDeliverEventArgs delivery)
		{
			messageContext.MessageId = delivery.ConsumerTag;

			//problems with message id
			//if (HandledMaximumRetries(messageContext.MessageId))
			//{
			//    MoveToPoison(delivery);
			//    channel.BasicAck(delivery.DeliveryTag, false);
			//    return;
			//}

			if (StartedMessageProcessing != null)
				StartedMessageProcessing(this, null);

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
				channel.BasicAck(delivery.DeliveryTag, false);
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
			var noErrorReceiving = OnTransportMessageReceived(m);
			var noErrorFinishing = OnFinishedMessageProcessing();

			if (messageContext.NeedToAbort)
				throw new AbortHandlingCurrentMessageException();

			if (!(noErrorReceiving && noErrorFinishing))
				throw new MessageHandlingException("Exception occured while processing message.");

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
			using (var channel = connectionProvider.Open(this.ProtocolName, errorAddress, false))
			{
				log.Info("Moving " + delivery.BasicProperties.MessageId + " to " + errorAddress);
				channel.BasicPublish(errorAddress.Exchange, errorAddress.QueueName, delivery.BasicProperties, delivery.Body);
			}
		}

		private bool OnFailedMessageProcessing()
		{
			try
			{
				if (FailedMessageProcessing != null)
					FailedMessageProcessing(this, null);
			}
			catch (Exception e)
			{
				log.Error("Failed raising 'failed message processing' event.", e);
				return false;
			}

			return true;
		}

		private bool OnFinishedMessageProcessing()
		{
			try
			{
				if (FinishedMessageProcessing != null)
					FinishedMessageProcessing(this, null);
			}
			catch (Exception e)
			{
				log.Error("Failed raising 'finished message processing' event.", e);
				return false;
			}
			return true;
		}

		private bool OnTransportMessageReceived(TransportMessage msg)
		{
			try
			{
				if (TransportMessageReceived != null)
					TransportMessageReceived(this, new TransportMessageReceivedEventArgs(msg));
			}
			catch (Exception e)
			{
				log.Error("Failed raising 'transport message received' event for message with ID=" + msg.Id, e);
				return false;
			}

			return true;
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

		public string InputUsername { get; set; }

		public string InputPassword { get; set; }

		public string InputVirtualHost { get; set; }
		public string ErrorPassword { get; set; }
		public string ErrorUsername { get; set; }
		public string ErrorVirtualHost { get; set; }

	}
}