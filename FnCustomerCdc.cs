using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Extensions.Sql;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace KelagPoc
{
    public class FnCustomerCdc
    {
        private readonly ILogger _logger;

        public FnCustomerCdc(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<FnCustomerCdc>();
        }

        [Function("FnCustomerCdc")]
        public DispatchedMessages Run(
            [SqlTrigger("[dbo].[Customer]", "SqlConnectionString")] IReadOnlyList<SqlChange<Customer>> changes)
        {
            var messages = new List<Customer>();
            _logger.LogInformation("C# SQL trigger function processed a request.");
            changes.ToList().ForEach(c =>
            {
                _logger.LogInformation(string.Format("Received change. Operation: {0}, Data: {1}", c.Operation, c.Item.ToString()));
                messages.Add(c.Item);
            });

            _logger.LogInformation("Sending changes to ServiceBus topic..");
            return new DispatchedMessages
            {
                Messages = messages.Select(x => JsonConvert.SerializeObject(x))
            };
        }
    }

    public class DispatchedMessages
    {
        [ServiceBusOutput(queueOrTopicName: "sb-customer-topic", entityType: ServiceBusEntityType.Topic)]
        public IEnumerable<string> Messages { get; set; }
    }

    public class Customer
    {
        public int ID { get; set; }
        public int SmartMeterId { get; set; }
        public string? FirstName { get; set; }
        public string? LastName { get; set; }
        public int Age { get; set; }
        public string? EmailAddress { get; set; }
        public string? PhoneNumber { get; set; }

        public override string ToString()
        {
            return base.ToString() + "(ID: " + ID + ", SmartMeterId: " + SmartMeterId + ", FirstName: " + FirstName +
                ", LastName: " + LastName + ", Age: " + Age + ", EmailAddress: " + EmailAddress + ", PhoneNumber: " + PhoneNumber + ")";
        }
    }
}
