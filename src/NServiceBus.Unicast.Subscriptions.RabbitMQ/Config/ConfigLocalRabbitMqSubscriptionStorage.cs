using NServiceBus.ObjectBuilder;

namespace NServiceBus.Unicast.Subscriptions.RabbitMQ.Config
{
	public static class ConfigureRabbitMq
	{
		public static ConfigLocalRabbitMqSubscriptionStorage LocalRabbitMqSubcriptionStorage(this Configure config)
		{
			var cfg = new ConfigLocalRabbitMqSubscriptionStorage();
			cfg.Configure(config);
			return cfg;
		}
	}

	public class ConfigLocalRabbitMqSubscriptionStorage : Configure
	{
		private IComponentConfig<LocalRabbitMqSubscriptionStorage> config;

		public void Configure(Configure config)
		{
			Builder = config.Builder;
			Configurer = config.Configurer;

			this.config = Configurer.ConfigureComponent<LocalRabbitMqSubscriptionStorage>(ComponentCallModelEnum.Singleton);
			var cfg = NServiceBus.Configure.GetConfigSection<LocalRabbitMqSubscriptionStorageConfig>();

			if(cfg != null)
			{
				//configure
			}
		}
	}
}