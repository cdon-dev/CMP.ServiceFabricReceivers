using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricReceiver.Common
{
    public static class Execution
    {
        public static async Task ExecuteAsync(
            CancellationToken cancellationToken, 
            ILogger logger,
            Action<string, object[]> serviceEventSource,
            string serviceName,
            string partitionId,
            Func<CancellationToken, Task> f)
        {
            try
            {
                await f(cancellationToken);
            }
            catch (Exception e) when (cancellationToken.IsCancellationRequested)
            {
                if (e is OperationCanceledException)
                {
                    logger.LogError(e, serviceName + " RunAsync canceled. RunAsync for {ServiceFabricPartitionId}", partitionId);
                    serviceEventSource($"{serviceName}.RunAsync for {partitionId} error {e}", new object[0]);
                    throw;
                }

                logger.LogError(e, serviceName + " Exception during shutdown. Exception of unexpected type .RunAsync for {ServiceFabricPartitionId}", partitionId);
                serviceEventSource($"{serviceName}.RunAsync for {partitionId} error {e}", new object[0]);
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (Exception e)
            {
                logger.LogError(e, serviceName + ". RunAsync for {ServiceFabricPartitionId}", partitionId);
                serviceEventSource($"{serviceName}.RunAsync for {partitionId} error {e}", new object[0]);
                throw;
            }
        }
    }
}
