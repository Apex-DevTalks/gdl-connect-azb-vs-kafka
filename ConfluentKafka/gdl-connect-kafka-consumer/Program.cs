using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;

class Program
{
	private static readonly CancellationTokenSource cts = new();

	static void Main(string[] args)
	{
		IConfiguration config = new ConfigurationBuilder()
		.SetBasePath(Directory.GetCurrentDirectory())
		.AddIniFile("client.properties", false)
		.Build();

		const string topic = "gdl-connect-kafka-topic";

		config["group.id"] = "csharp-group-1";
		config["auto.offset.reset"] = "earliest";
		

		Console.CancelKeyPress += (sender, eventArgs) =>
		{
			Console.WriteLine("Ctrl+C has been pressed. Initiating shutdown process...");
			cts.Cancel();
			eventArgs.Cancel = true;
		};

		Console.WriteLine("Application has started. Ctrl-C to end");

        // creates a new consumer instance
        using var consumer = new ConsumerBuilder<string, string>(config.AsEnumerable()).Build();
        consumer.Subscribe(topic);
		
        while (!cts.Token.IsCancellationRequested)
        {
            // consumes messages from the subscribed topic and prints them to the console
            try
			{
				var cr = consumer.Consume(cts.Token);
	            Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
			}
			catch (OperationCanceledException) { }
        }

        // closes the consumer connection
        consumer.Close();

		Console.WriteLine("The consumer was closed correctly");
		Console.WriteLine("Now shutting down");
	}
}