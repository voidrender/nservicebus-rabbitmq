using System;
using System.Transactions;
using log4net;
using RabbitMQ.Client;

namespace NServiceBus.Unicast.Transport.RabbitMQ
{
	public class RabbitMqEnlistment : IEnlistmentNotification
	{
		private readonly ILog log = LogManager.GetLogger(typeof (RabbitMqEnlistment));
		private readonly OpenedSession openedSession;

		public RabbitMqEnlistment(OpenedSession openedSession)
		{
			this.openedSession = openedSession;
		}

		public void Prepare(PreparingEnlistment preparingEnlistment)
		{
			log.Debug("Prepared");
			preparingEnlistment.Prepared();
		}

		public void Commit(Enlistment enlistment)
		{
			log.Debug("Commit");
			openedSession.Dispose();
			enlistment.Done();
		}

		public void Rollback(Enlistment enlistment)
		{
			log.Debug("Rollback");
			openedSession.Dispose();
			enlistment.Done();
		}

		public void InDoubt(Enlistment enlistment)
		{
			log.Debug("Doubt");
		}
	}

	public class OpenedSession : IDisposable
	{
		private readonly IConnection connection;
		private readonly IModel model;
		private Int32 refs;
		public event EventHandler Disposed;

		public bool IsActive
		{
			get { return refs > 0; }
		}

		public OpenedSession(IConnection connection, IModel model)
		{
			this.connection = connection;
			this.model = model;
		}

		public OpenedSession AddRef()
		{
			refs++;
			return this;
		}

		public void Dispose()
		{
			if (--refs == 0)
			{
				model.Close(200, "Ok");
				model.Dispose();
				connection.Close();
				connection.Dispose();
				Disposed(this, EventArgs.Empty);
			}
		}

		public IModel Model()
		{
			return model;
		}

		public IConnection Connection
		{
			get { return connection; }
		}
	}
}