using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using StackExchange.Redis;
using Microsoft.Data.SqlClient;
using System.Data;
using System.Text.Json;
using System.Linq;

namespace DBSyncService
{
    public class Worker : BackgroundService
    {
        private readonly ILogger<Worker> _logger;
        private readonly IConfiguration _configuration;
        private readonly ConnectionMultiplexer _redisConn;
        private readonly IDatabase _redisDb;
        private readonly int _interval;
        private readonly int _topRows;
        private readonly int _retryWaitMinutes;
        private readonly List<string> _sqlConnectionStrings;
        private readonly bool _takeDataFromTables;
        private readonly Dictionary<string, string> _tableKeyMapping; 

        public Worker(ILogger<Worker> logger, IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;

            _interval = _configuration.GetValue<int>("WorkerSettings:SyncIntervalSeconds");
            _topRows = _configuration.GetValue<int>("WorkerSettings:TopRows");
            _retryWaitMinutes = 10;
            _takeDataFromTables = _configuration.GetValue<bool>("WorkerSettings:TakeDataFromTable");

            _sqlConnectionStrings = _configuration.GetSection("ConnectionStrings")
                                                 .GetChildren()
                                                 .Where(x => x.Key.StartsWith("SqlServer"))
                                                 .Select(x => x.Value)
                                                 .ToList();

            _redisConn = ConnectionMultiplexer.Connect(_configuration.GetValue<string>("Redis"));
            _redisDb = _redisConn.GetDatabase();

            // Define table-specific key columns if not using PK
            _tableKeyMapping = _configuration.GetSection("WorkerSettings:TableKeyMapping")
                                            .Get<Dictionary<string, string>>()
                                            ?? new Dictionary<string, string>();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                _logger.LogInformation("=== Sync cycle started at {time} ===", DateTimeOffset.Now);

                foreach (var connStr in _sqlConnectionStrings)
                {
                    string dbName = "";
                    try
                    {
                        await using var sqlConn = new SqlConnection(connStr);
                        await sqlConn.OpenAsync(stoppingToken);
                        dbName = sqlConn.Database;
                        _logger.LogInformation("Connected to database: {dbName}", dbName);

                        var sources = new List<(string Schema, string Name, bool IsTable)>();
                        if (_takeDataFromTables)
                        {
                            await using var tableCmd = new SqlCommand(
                                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE'", sqlConn);
                            await using var reader = await tableCmd.ExecuteReaderAsync(stoppingToken);
                            while (await reader.ReadAsync(stoppingToken))
                                sources.Add((reader.GetString(0), reader.GetString(1), true));
                        }
                        else
                        {
                            await using var viewCmd = new SqlCommand(
                                "SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS", sqlConn);
                            await using var reader = await viewCmd.ExecuteReaderAsync(stoppingToken);
                            while (await reader.ReadAsync(stoppingToken))
                                sources.Add((reader.GetString(0), reader.GetString(1), false));
                        }

                        _logger.LogInformation("{count} sources found in database {dbName}", sources.Count, dbName);

                        foreach (var source in sources)
                        {
                            _logger.LogInformation("Syncing source: {name}", source.Name);

                            var primaryKeys = source.IsTable ? await GetPrimaryKeysAsync(sqlConn, source.Schema, source.Name) : new List<string>();

                            string fetchQuery = $@"
                                SELECT TOP ({_topRows}) *
                                FROM [{source.Schema}].[{source.Name}]";

                            DataTable dt = new DataTable();
                            await using var cmd = new SqlCommand(fetchQuery, sqlConn);
                            using var adapter = new SqlDataAdapter(cmd);
                            adapter.Fill(dt);

                            if (dt.Rows.Count == 0)
                            {
                                _logger.LogInformation("No rows found for source: {name}", source.Name);
                                continue;
                            }

                            string redisKeyColumn = null;

                            if (primaryKeys.Count > 0)
                            {
                                redisKeyColumn = null; 
                            }
                            else if (_tableKeyMapping.ContainsKey(source.Name))
                            {
                                redisKeyColumn = _tableKeyMapping[source.Name];
                            }
                            else
                            {
                                var numericOrDateCol = dt.Columns.Cast<DataColumn>()
                                    .FirstOrDefault(c => c.DataType == typeof(int)
                                                      || c.DataType == typeof(long)
                                                      || c.DataType == typeof(DateTime));
                                redisKeyColumn = numericOrDateCol?.ColumnName ?? dt.Columns[0].ColumnName;
                            }

                            _logger.LogInformation("Syncing {rowCount} rows from source {name}", dt.Rows.Count, source.Name);

                            var tasks = dt.AsEnumerable().Select(async row =>
                            {
                                var dict = dt.Columns.Cast<DataColumn>()
                                    .ToDictionary(col => col.ColumnName, col => row[col]);

                                string redisKey = primaryKeys.Count > 0
                                    ? $"{dbName}:{source.Name}:{string.Join(":", primaryKeys.Select(pk => row[pk]))}"
                                    : $"{dbName}:{source.Name}:{row[redisKeyColumn]}";

                                var json = JsonSerializer.Serialize(dict);
                                var compressed = AdvancedCompressor.CompressString(json);

                                while (true)
                                {
                                    try
                                    {
                                        await _redisDb.StringSetAsync(redisKey, compressed, TimeSpan.FromHours(1));
                                        break;
                                    }
                                    catch (RedisServerException ex) when (ex.Message.Contains("OOM"))
                                    {
                                        _logger.LogWarning("Redis OOM: waiting {minutes} minutes before retry...", _retryWaitMinutes);
                                        await Task.Delay(TimeSpan.FromMinutes(_retryWaitMinutes), stoppingToken);
                                    }
                                }
                            });

                            await Task.WhenAll(tasks);
                            _logger.LogInformation("Completed syncing source: {name}", source.Name);
                        }

                        _logger.LogInformation("Completed syncing database: {dbName}", dbName);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError(ex, "Error syncing database {dbName}", dbName);
                    }
                }

                _logger.LogInformation("=== Sync cycle completed at {time} ===", DateTimeOffset.Now);
                await Task.Delay(_interval * 1000, stoppingToken);
            }
        }

        private static async Task<List<string>> GetPrimaryKeysAsync(SqlConnection conn, string schema, string table)
        {
            var cmdText = @"
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                  ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                WHERE tc.TABLE_SCHEMA = @Schema
                  AND tc.TABLE_NAME = @Table
                  AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                ORDER BY kcu.ORDINAL_POSITION";

            using var cmd = new SqlCommand(cmdText, conn);
            cmd.Parameters.AddWithValue("@Schema", schema);
            cmd.Parameters.AddWithValue("@Table", table);

            var keys = new List<string>();
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
                keys.Add(reader.GetString(0));

            return keys;
        }
    }
}
