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
            ILogger _logger,
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
                    _logger.LogError(e, serviceName + " RunAsync canceled. RunAsync for {PartitionId}", partitionId);
                    serviceEventSource($"{serviceName}.RunAsync for {partitionId} error {e}", new object[0]);
                    throw;
                }

                _logger.LogError(e, serviceName + " Exception during shutdown. Exception of unexpected type .RunAsync for {PartitionId}", partitionId);
                serviceEventSource($"{serviceName}.RunAsync for {partitionId} error {e}", new object[0]);
                cancellationToken.ThrowIfCancellationRequested();
            }
            catch (Exception e)
            {
                _logger.LogError(e, serviceName + ". RunAsync for {PartitionId}", partitionId);
                serviceEventSource($"{serviceName}.RunAsync for {partitionId} error {e}", new object[0]);
                throw;
            }
        }
    }
}
