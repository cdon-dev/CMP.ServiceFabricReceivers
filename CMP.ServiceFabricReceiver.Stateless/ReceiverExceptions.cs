using CMP.ServiceFabricReceiver.Common;
using Microsoft.Azure.EventHubs;
using Microsoft.Azure.EventHubs.Processor;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CMP.ServiceFabricRecevier.Stateless
{

    public static class ReceiverExceptions
    {
        public static async Task ExecuteAsync(
            CancellationToken cancellationToken,
            ILogger logger,
            string partitionId,
            Func<CancellationToken, Task> f)
        {
            try
            {
                await f(cancellationToken);
            }
            catch (ReceiverDisconnectedException e)
            {
                logger.LogInformation($"Execution.ExecuteAsync error 1 {e.GetType().Name}");
                logger.LogInformation(
                    "Receiver disconnected on partition {PartitionId}. " +
                    "Exception: {@Exception}, IsCancellationRequested: {IsCancellationRequested}",
                    partitionId, e, cancellationToken.IsCancellationRequested);
                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
                throw;
            }
            catch (LeaseLostException e)
            {
                logger.LogInformation($"Execution.ExecuteAsync error 2 {e.GetType().Name}");
                logger.LogInformation(
                    "Lease lost on partition {PartitionId}. " +
                    "Exception: {@Exception}, IsCancellationRequested: {IsCancellationRequested}",
                    partitionId, e, cancellationToken.IsCancellationRequested);
                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);
                throw;
            }

        }
    }
}
