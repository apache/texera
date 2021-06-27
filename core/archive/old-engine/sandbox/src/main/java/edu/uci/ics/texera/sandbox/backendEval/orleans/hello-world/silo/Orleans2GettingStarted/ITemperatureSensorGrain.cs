using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Orleans2GettingStarted
{
    public interface ITemperatureSensorGrain : IGrainWithIntegerKey
    {
        Task SubmitTemperatureAsync(float temperature);
    }
}
