namespace project;

using dotenv.net;
using Ydb.Sdk;
using Ydb.Sdk.Services.Table;

class Program
{
    static void Main(string[] args)
    {
        DotEnv.Load();
        string? IAMToken = Environment.GetEnvironmentVariable("IAM_TOKEN");
        string? endpoint = Environment.GetEnvironmentVariable("ENDPOINT");
        string? database = Environment.GetEnvironmentVariable("DATABASE");
        if (IAMToken == null || endpoint == null || database == null)
        {
            Console.WriteLine("Переменные окружения не инициализированы");
            Environment.Exit(1);
        }
        InitDBConnection(IAMToken, database, endpoint).Wait();
    }

    static async Task InitDBConnection(string iamtoken, string database, string endpoint)
    {
        //подключение к YDB
        var config = new DriverConfig(
            //ссылка на YDB
            endpoint: endpoint,
            //путь к YDB
            database: database,
            //получение доступа к YDB через токен
            credentials: new Ydb.Sdk.Auth.TokenProvider(iamtoken)
        );

        await using var driver = await Driver.CreateInitialized(config);

        using var tableClient = new TableClient(driver, new TableClientConfig());

        var response = await tableClient.SessionExec(async session =>
        { //запрос к полям таблицы
            var query =
                @"
                DECLARE $id AS Uint64;
                DECLARE $name AS String;

                SELECT id, name
                FROM clients;
            ";

            return await session.ExecuteDataQuery(
                query: query,
                txControl: TxControl.BeginSerializableRW().Commit()
            );
        });

        response.Status.EnsureSuccess();
        var queryResponse = (ExecuteDataQueryResponse)response;
        var resultSet = queryResponse.Result.ResultSets[0];
        //вывод результата запроса в консоль
        Console.WriteLine(resultSet.Rows[0]["name"]);
    }
}
