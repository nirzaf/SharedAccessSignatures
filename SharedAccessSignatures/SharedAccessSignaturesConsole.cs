using Microsoft.Azure.ServiceBus;
using Microsoft.Azure.ServiceBus.Management;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SharedAccessSignatures
{
    class SharedAccessSignaturesConsole
    {
        //static string SbNamespace = "sbdemos";
        static string TopicPath = "ordertopicsecurity";

        static TopicClient OrderTopicclient;

        static async Task Main(string[] args)
        {

            Console.ForegroundColor = ConsoleColor.Cyan;

            
            await CreateTopicAndSubscriptions();
            Console.WriteLine("Done!");

            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("Press enter to send messages");
            Console.ReadLine();


            var headOfficeConnectionString = System.Configuration.ConfigurationManager.AppSettings["headOffice"];
            //var managementClient = new ManagementClient(headOfficeConnectionString);

            //OrderTopicclient = await managementClient.CreateTopicAsync(TopicPath);

            OrderTopicclient = new TopicClient(headOfficeConnectionString, TopicPath);



            Console.WriteLine("Sending orders...");


            // Send five orders with different properties.
            await SendOrder(new Order()
            {
                Name = "Loyal Customer",
                Value = 19.99,
                Region = "USA",
                Items = 1,
                HasLoyltyCard = true
            });

            await SendOrder(new Order()
            {
                Name = "Large Order",
                Value = 49.99,
                Region = "USA",
                Items = 50,
                HasLoyltyCard = false
            });

            await SendOrder(new Order()
            {
                Name = "High Value Order",
                Value = 749.45,
                Region = "USA",
                Items = 45,
                HasLoyltyCard = false
            });

            await SendOrder(new Order()
            {
                Name = "Loyal Europe Order",
                Value = 49.45,
                Region = "EU",
                Items = 3,
                HasLoyltyCard = true
            });

            await SendOrder(new Order()
            {
                Name = "UK Order",
                Value = 49.45,
                Region = "UK",
                Items = 3,
                HasLoyltyCard = false
            });

            try
            {
                // Close the TopicClient.
                await OrderTopicclient.CloseAsync();
            }
            catch (UnauthorizedAccessException ex)
            {

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
            }


            Console.ForegroundColor = ConsoleColor.White;
            Console.WriteLine("Press enter to receive messages");
            Console.ReadLine();

            // Receive all messages from the ordertopic subscriptions.
            await ReceiveFromSubscriptions(TopicPath);




            Console.ReadLine();

        }


        static async Task CreateTopicAndSubscriptions()
        {
            Console.WriteLine("Recreate topic and subscriptions?");
            var response = Console.ReadLine();
            if (!response.ToLower().StartsWith("y"))
            {
                return;
            }

            
            var headOfficeConnectionString = System.Configuration.ConfigurationManager.AppSettings["headoffice"];

            var managementClient = new ManagementClient(headOfficeConnectionString);



            try
            {
                // Delete and recreate the topic
                if (await managementClient.TopicExistsAsync(TopicPath))
                {
                    await managementClient.DeleteTopicAsync(TopicPath);
                }
                await managementClient.CreateTopicAsync(TopicPath);

                // Subscriptions for the regions
                await managementClient.CreateSubscriptionAsync(new SubscriptionDescription(TopicPath, "usaSubscription"),
                    new RuleDescription("Default", new SqlFilter("Region = 'USA'")));

                await managementClient.CreateSubscriptionAsync(new SubscriptionDescription(TopicPath, "euSubscription"),
                    new RuleDescription("Default", new SqlFilter("Region = 'EU'")));

                await managementClient.CreateSubscriptionAsync(new SubscriptionDescription(TopicPath, "ukSubscription"),
                    new RuleDescription("Default", new SqlFilter("Region = 'UK'")));


            }
            catch (UnauthorizedAccessException ex)
            {

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
            }
        }




        static async Task SendOrder(Order order)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.Write("Sending {0}...", order.Name);

            var orderJson = JsonConvert.SerializeObject(order);

            // Create a message from the order.
            Message orderMsg = new Message(Encoding.UTF8.GetBytes(orderJson));

            // Promote properties.
            orderMsg.UserProperties.Add("Region", order.Region);

            try
            {
                // Send the message.
                await OrderTopicclient.SendAsync(orderMsg);
            }
            catch (UnauthorizedAccessException ex)
            {
                Console.WriteLine();
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
            }

            Console.WriteLine("Done!");
        }

        private static async Task ReceiveFromSubscriptions(string topicPath)
        {
            Console.ForegroundColor = ConsoleColor.Magenta;
            Console.WriteLine("Receiving from topic {0} subscriptions.", topicPath);


            try
            {

                var headOfficeConnectionString = System.Configuration.ConfigurationManager.AppSettings["headOffice"];

                var managementClient = new ManagementClient(headOfficeConnectionString);

                var subscriptions = await managementClient.GetSubscriptionsAsync(TopicPath);

                // Loop through the subscriptions in a topic.
                foreach (var subDescription in subscriptions)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    


                    var subOfficeConnectionString = System.Configuration.ConfigurationManager.AppSettings[subDescription.SubscriptionName];

                    // Create a SubscriptionClient
                    var subOfficeSubClient = new SubscriptionClient(subOfficeConnectionString, TopicPath, subDescription.SubscriptionName);

                    try
                    {
                        var messageHandlerOptions = new MessageHandlerOptions(ExceptionReceivedHandler)
                        {
                            MaxConcurrentCalls = 1,
                            AutoComplete = true
                        };
                        subOfficeSubClient.RegisterMessageHandler(ProcessOrderMessageMessageAsync, messageHandlerOptions);
                        Console.WriteLine($"Receiving from { subDescription.SubscriptionName }.");
                        Console.ReadLine();

                        //// Receive all the massages form the subscription.
                        //Console.ForegroundColor = ConsoleColor.Green;
                        //while (true)
                        //{
                        //    // Recieve any message with a one second timeout.
                        //    BrokeredMessage msg = subOfficeSubClient.Receive(TimeSpan.FromSeconds(1));
                        //    if (msg != null)
                        //    {
                        //        // Deserialize the message body to an order.
                        //        Order order = msg.GetBody<Order>();
                        //        Console.WriteLine("    Name {0} {1} items {2} ${3} {4}",
                        //            order.Name, order.Region, order.Items, order.Value,
                        //            order.HasLoyltyCard ? "Loyal" : "Not loyal");

                        //        // Mark the message as complete.
                        //        msg.Complete();
                        //    }
                        //    else
                        //    {
                        //        break;
                        //    }
                        //}


                        // Close the SubscriptionClient.
                        await subOfficeSubClient.CloseAsync();
                    }
                    catch (UnauthorizedAccessException ex)
                    {

                        Console.ForegroundColor = ConsoleColor.Red;
                        Console.WriteLine(ex.Message);
                    }
                }
            }
            catch (UnauthorizedAccessException ex)
            {

                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(ex.Message);
            }
            Console.ResetColor();
        }
        private static async Task ProcessOrderMessageMessageAsync(Message message, CancellationToken token)
        {
            // Process the order message
            var orderJson = Encoding.UTF8.GetString(message.Body);
            var order = JsonConvert.DeserializeObject<Order>(orderJson);

            Console.WriteLine($"{ order.ToString() }");

        }

        private static Task ExceptionReceivedHandler(ExceptionReceivedEventArgs exceptionReceivedEventArgs)
        {
            Console.WriteLine($"Message handler encountered an exception {exceptionReceivedEventArgs.Exception}.");
            return Task.CompletedTask;
        }



    }
}
