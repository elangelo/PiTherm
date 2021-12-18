using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core;

var influxDBClient = InfluxDBClientFactory.Create("http://bors.local:8086");
var i = 0;
while (true)
{
    if (i % 20 == 0)
    {
        //some logging everynow and then allows some feedback
        System.Console.WriteLine($"{DateTime.UtcNow}: added info to Influx");
        System.Console.WriteLine("getting temps!");
    }
    var basePathSensors = "/sys/bus/w1/devices/";
    var sensors = new Dictionary<string, string>{
        {"bottom", "28-021581a9cdff"},
        {"top","28-021581d6f1ff"},
        {"in","28-0215819713ff"},
        {"out","28-0115818cf0ff"}
    };

    using (var writeApi = influxDBClient.GetWriteApi())
    {
        var tempMeasurement = new Temperature
        {
            Time = DateTime.UtcNow
        };

        foreach (var key in sensors.Keys)
        {
            var temp = GetTemp(Path.Join(basePathSensors, sensors[key], "temperature"));
            switch (key)
            {
                case "bottom":
                    tempMeasurement.Bottom = temp;
                    break;
                case "top":
                    tempMeasurement.Top = temp;
                    break;
                case "in":
                    tempMeasurement.In = temp;
                    break;
                case "out":
                    tempMeasurement.Out = temp;
                    break;
                default:
                    break;

            }
        }
        writeApi.WriteMeasurement("pithermserver", "org_id", WritePrecision.Ms, tempMeasurement);
        
        influxDBClient.Dispose();
    }
    i++;
    await Task.Delay(TimeSpan.FromMinutes(5));
}

double GetTemp(string sensorPath)
{
    var sensorV = System.IO.File.ReadAllText(sensorPath);
    return double.Parse(sensorV) / 1000;
}

[Measurement("temperature")]
public class Temperature
{
    [Column("bottom")] public double Bottom { get; set; }
    [Column("top")] public double Top { get; set; }
    [Column("in")] public double In { get; set; }
    [Column("out")] public double Out { get; set; }

    [Column(IsTimestamp = true)] public DateTime Time { get; set; }
}