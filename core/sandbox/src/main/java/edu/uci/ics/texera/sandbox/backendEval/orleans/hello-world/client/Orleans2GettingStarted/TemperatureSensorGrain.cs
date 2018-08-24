using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans2GettingStarted
{
    public class TemperatureSensorGrain : Grain, ITemperatureSensorGrain
    {
        public Task SubmitTemperatureAsync(float temperature)
        {
            long grainId = this.GetPrimaryKeyLong();
            Console.WriteLine($"{grainId} received temperature: {temperature}");

            return Task.CompletedTask;
        }
    }
}
