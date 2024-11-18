using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using MassTransit;
using Microsoft.Extensions.DependencyInjection;
using System.Threading;

namespace mtworkertest;

public record TestJob();
public class TestJobConsumer : IJobConsumer<TestJob>
{
  public Task Run(JobContext<TestJob> context)
  {
    throw new Exception("Some failure");
  }
}

public class ScheduledJobInitializer(IServiceProvider serviceProvider) : IHostedLifecycleService
{
  public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;
  public async Task StartedAsync(CancellationToken cancellationToken)
  {
    using var scope = serviceProvider.CreateScope();
    var endpoint = scope.ServiceProvider.GetRequiredService<IPublishEndpoint>();
    await endpoint.AddOrUpdateRecurringJob(
      "test-job",
      new TestJob(),
      x => x.Every(minutes: 1),
      cancellationToken);
  }
  public Task StartingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
  public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
  public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;
  public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}

public class Program
{
  public static async Task Main(string[] args)
  {
    await CreateHostBuilder(args).Build().RunAsync();
  }

  public static IHostBuilder CreateHostBuilder(string[] args) =>
    Host.CreateDefaultBuilder(args)
      .ConfigureServices((hostContext, services) =>
      {
        services.AddMassTransit(x =>
          {
            x.SetKebabCaseEndpointNameFormatter();

            // By default, sagas are in-memory, but should be changed to a durable
            // saga repository.
            x.SetInMemorySagaRepositoryProvider();
            x.AddJobSagaStateMachines();
            x.AddDelayedMessageScheduler();

            x.AddConsumer<TestJobConsumer>(c =>
              c.Options<JobOptions<TestJob>>(options =>
                options.SetConcurrentJobLimit(1)));

            x.UsingInMemory((context, cfg) =>
              {
                cfg.UseDelayedMessageScheduler();
                cfg.ConfigureEndpoints(context);
              });
          });
        services.AddHostedService<ScheduledJobInitializer>();
        services.AddOptions<MassTransitHostOptions>()
          .Configure(options =>
          {
            options.WaitUntilStarted = true;
            options.StartTimeout = TimeSpan.FromMinutes(1);
            options.StopTimeout = TimeSpan.FromMinutes(1);
          });
      });
}
