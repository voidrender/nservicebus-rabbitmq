using System;
using System.Runtime.Serialization;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	public class MessageHandlingException : Exception
	{
		private readonly Exception startedProcessingException;
		private readonly Exception receivingException;
		private readonly Exception finishedProcessingException;

		public Exception StartedProcessingException
		{
			get { return startedProcessingException; }
		}

		public Exception ReceivingException
		{
			get { return receivingException; }
		}

		public Exception FinishedProcessingException
		{
			get { return finishedProcessingException; }
		}

		public MessageHandlingException() {}

		public MessageHandlingException(string message, Exception startedProcessingException, Exception receivingException,
		                                Exception finishedProcessingException)
			: base(message)
		{
			this.startedProcessingException = startedProcessingException;
			this.receivingException = receivingException;
			this.finishedProcessingException = finishedProcessingException;
		}

		public MessageHandlingException(string message)
			: base(message) {}

		public MessageHandlingException(string message, Exception innerException)
			: base(message, innerException) {}

		protected MessageHandlingException(SerializationInfo info, StreamingContext context)
			: base(info, context) {}
	}
}