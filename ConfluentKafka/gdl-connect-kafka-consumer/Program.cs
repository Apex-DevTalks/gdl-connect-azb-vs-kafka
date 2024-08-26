using System;
using System.IO;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;

class Program {
  public static IConfiguration readConfig() {
    // reads the client configuration from client.properties
    // and returns it as a configuration object
    return new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddIniFile("client.properties", false)
    .Build();
  }

  public static void consume (string topic, IConfiguration config) {
    config["group.id"] = "csharp-group-1";
    config["auto.offset.reset"] = "earliest";

    // creates a new consumer instance
    using (var consumer = new ConsumerBuilder<string, string>(config.AsEnumerable()).Build()) {
      consumer.Subscribe(topic);
      while (true) {
        // consumes messages from the subscribed topic and prints them to the console
        var cr = consumer.Consume();
        Console.WriteLine($"Consumed event from topic {topic}: key = {cr.Message.Key,-10} value = {cr.Message.Value}");
      }

      // closes the consumer connection
      consumer.Close();
    }
  }

  public static void Main (string[] args) {
    // producer and consumer code here
    IConfiguration config = readConfig();
    const string topic = "gdl-connect-kafka-topic";
    
    consume(topic, config); 
  }
}