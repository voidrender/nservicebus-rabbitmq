using System;
using System.Configuration;

namespace NServiceBus.Unicast.Transport.RabbitMQ.Config
{
	public class RabbitMqTransportConfig : ConfigurationSection
	{
		#region Input Config

		[ConfigurationProperty("InputBroker", IsRequired = true)]
		public string InputBroker
		{
			get { return base["InputBroker"] as string; }
			set { base["InputBroker"] = value; }
		}

		[ConfigurationProperty("InputExchange", IsRequired = false, DefaultValue = "")]
		public string InputExchange
		{
			get { return base["InputExchange"] as string; }
			set { base["InputExchange"] = value; }
		}

		[ConfigurationProperty("InputExchangeType", IsRequired = false, DefaultValue = "direct")]
		public string InputExchangeType
		{
			get { return base["InputExchangeType"] as string; }
			set { base["InputExchangeType"] = value; }
		}
		
		[ConfigurationProperty("InputQueue", IsRequired = false)]
		public string InputQueue
		{
			get { return base["InputQueue"] as string; }
			set { base["InputQueue"] = value; }
		}

		[ConfigurationProperty("InputRoutingKeys", IsRequired = false)]
		public string InputRoutingKeys
		{
			get { return base["InputRoutingKeys"] as string; }
			set { base["InputRoutingKeys"] = value; }
		}

		[ConfigurationProperty("InputIsDurable", IsRequired = false, DefaultValue = false)]
		public bool InputIsDurable
		{
			get { return (bool) base["InputIsDurable"]; }
			set { base["InputIsDurable"] = value; }
		}

		[ConfigurationProperty("DoNotCreateInputExchange", IsRequired = false, DefaultValue = false)]
		public bool DoNotCreateInputExchange
		{
			get { return (bool)base["DoNotCreateInputExchange"]; }
			set { base["DoNotCreateInputExchange"] = value; }
		}

		[ConfigurationProperty("DoNotCreateInputQueue", IsRequired = false, DefaultValue = false)]
		public bool DoNotCreateInputQueue
		{
			get { return (bool)base["DoNotCreateInputQueue"]; }
			set { base["DoNotCreateInputQueue"] = value; }
		}

		[ConfigurationProperty("InputUsername", IsRequired = false)]
		public string InputUsername
		{
			get { return base["InputUsername"] as string; }
			set { base["InputUsername"] = value; }
		}

		[ConfigurationProperty("InputPassword", IsRequired = false)]
		public string InputPassword
		{
			get { return base["InputPassword"] as string; }
			set { base["InputPassword"] = value; }
		}

		[ConfigurationProperty("InputVirtualHost", IsRequired = false)]
		public string InputVirtualHost
		{
			get { return base["InputVirtualHost"] as string; }
			set { base["InputVirtualHost"] = value; }
		}
		#endregion

		#region Error Config

		[ConfigurationProperty("ErrorBroker", IsRequired = true)]
		public string ErrorBroker
		{
			get { return base["ErrorBroker"] as string; }
			set { base["ErrorBroker"] = value; }
		}

		[ConfigurationProperty("ErrorExchange", IsRequired = false, DefaultValue = "")]
		public string ErrorExchange
		{
			get { return base["ErrorExchange"] as string; }
			set { base["ErrorExchange"] = value; }
		}

		[ConfigurationProperty("ErrorExchangeType", IsRequired = false, DefaultValue = "fanout")]
		public string ErrorExchangeType
		{
			get { return base["ErrorExchangeType"] as string; }
			set { base["ErrorExchangeType"] = value; }
		}

		[ConfigurationProperty("ErrorQueue", IsRequired = false)]
		public string ErrorQueue
		{
			get { return base["ErrorQueue"] as string; }
			set { base["ErrorQueue"] = value; }
		}

		[ConfigurationProperty("ErrorRoutingKeys", IsRequired = false)]
		public string ErrorRoutingKeys
		{
			get { return base["ErrorRoutingKeys"] as string; }
			set { base["ErrorRoutingKeys"] = value; }
		}

		[ConfigurationProperty("DoNotCreateErrorExchange", IsRequired = false, DefaultValue = true)]
		public bool DoNotCreateErrorExchange
		{
			get { return (bool)base["DoNotCreateErrorExchange"]; }
			set { base["DoNotCreateErrorExchange"] = value; }
		}

		[ConfigurationProperty("DoNotCreateErrorQueue", IsRequired = false, DefaultValue = true)]
		public bool DoNotCreateErrorQueue
		{
			get { return (bool)base["DoNotCreateErrorQueue"]; }
			set { base["DoNotCreateErrorQueue"] = value; }
		}

		[ConfigurationProperty("ErrorUsername", IsRequired = false)]
		public string ErrorUsername
		{
			get { return base["ErrorUsername"] as string; }
			set { base["ErrorUsername"] = value; }
		}

		[ConfigurationProperty("ErrorPassword", IsRequired = false)]
		public string ErrorPassword
		{
			get { return base["ErrorPassword"] as string; }
			set { base["ErrorPassword"] = value; }
		}

		[ConfigurationProperty("ErrorVirtualHost", IsRequired = false)]
		public string ErrorVirtualHost
		{
			get { return base["ErrorVirtualHost"] as string; }
			set { base["ErrorVirtualHost"] = value; }
		}
		
		[ConfigurationProperty("ErrorIsDurable", IsRequired = false, DefaultValue = false)]
		public bool ErrorIsDurable
		{
			get { return (bool) base["ErrorIsDurable"]; }
			set { base["ErrorIsDurable"] = value; }
		}

		#endregion

		[ConfigurationProperty("MaxRetries", IsRequired = true)]
		public int MaxRetries
		{
			get { return (int) base["MaxRetries"]; }
			set { base["MaxRetries"] = value; }
		}

		[ConfigurationProperty("NumberOfWorkerThreads", IsRequired = true)]
		public int NumberOfWorkerThreads
		{
			get { return (int) base["NumberOfWorkerThreads"]; }
			set { base["NumberOfWorkerThreads"] = value; }
		}

		[ConfigurationProperty("TransactionTimeout", IsRequired = false, DefaultValue = 5)]
		public int TransactionTimeout
		{
			get { return (int)base["TransactionTimeout"]; }
			set { base["TransactionTimeout"] = value; }
		}

		[ConfigurationProperty("SendAcknowledgement", IsRequired = false, DefaultValue = true)]
		public bool SendAcknowledgement
		{
			get { return (bool) base["SendAcknowledgement"]; }
			set { base["SendAcknowledgement"] = value; }
		}

	}
}