using InfluxDB.Client;
using InfluxDB.Client.Api.Domain;
using InfluxDB.Client.Core;
using Microsoft.Data.Sqlite;

// See https://aka.ms/new-console-template for more information

var db = "/home/samuel/source/migrate/piTemps.db";
var connString = $"Data Source={db}";

SqliteConnection conn = new SqliteConnection(connString);
conn.Open();
var influxDBClient = InfluxDBClientFactory.Create("http://bors.local:8086");
WriteTemps(conn, influxDBClient, 0, 100, 0);

void WriteTemps(SqliteConnection connection, InfluxDBClient influxDBClient, long starttime, int batchsize, int index)
{
    using var command = conn.CreateCommand();
    command.CommandText = "SELECT [unix_time],[bottom],[top],[in],[out] FROM (SELECT * FROM temperature_records WHERE unix_time > $starttime ORDER BY unix_time ASC LIMIT $limit) ORDER BY unix_time ASC;";
    command.Parameters.AddWithValue("$limit", 100);
    command.Parameters.AddWithValue("$starttime", starttime);
    var newstartime = starttime;

    using (var reader = command.ExecuteReader())
    using (var writeApi = influxDBClient.GetWriteApi())
    {
        if (!reader.HasRows)
        {
            System.Console.WriteLine(index);
            System.Console.WriteLine("no rows!");
            Environment.Exit(0);
        }
        while (reader.Read())
        {
            index++;
            var time = reader.GetInt64(0);
            var bottom = reader.GetFloat(1);
            var top = reader.GetFloat(2);
            var in_ = reader.GetFloat(3);
            var out_ = reader.GetFloat(4);

            var datetime = DateTimeOffset.FromUnixTimeMilliseconds(time).UtcDateTime;
            newstartime = time;
            // Console.WriteLine($"{datetime}, {time}: {bottom}, {top}, {in_}, {out_}");
            //
            // Write by POCO
            //
            var temperature = new Temperature
            {
                Bottom = bottom,
                Top = top,
                In = in_,
                Out = out_,
                Time = datetime
            };

            writeApi.WriteMeasurement("pithermserver", "org_id", WritePrecision.Ms, temperature);
        }
        influxDBClient.Dispose();
    }

    System.Console.WriteLine(index);
    Thread.Sleep(10000);
    WriteTemps(conn, influxDBClient, newstartime, batchsize, index);
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