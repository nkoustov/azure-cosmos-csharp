using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

public class Program
{
    // Replace <documentEndpoint> with the information created earlier
    private static readonly string EndpointUri = "https://nikolai-cosmos-cli.documents.azure.com:443";

    // Set variable to the Primary Key from earlier.
    private static readonly string PrimaryKey = "xS8ORATpDzYsMuh61G99ungfTlTNOhpQr0fS44FKK0czOYionIPLdfeljzCZG0qzMtc0nDBarm0cACDbbxBjrg==";

    // The Cosmos client instance
    private CosmosClient cosmosClient;

    // The database we will create
    private Database database;

    // The container we will create.
    private Container container;

    // The names of the database and container we will create
    private string databaseId = "az204Database";
    private string containerId = "az204Container";
    private string partitionKeyPath = "/AccountNumber";

    public static async Task Main(string[] args)
    {
        try
        {
            Console.WriteLine("Beginning operations...\n");
            Program p = new Program();
            await p.CosmosAsync();

        }
        catch (CosmosException de)
        {
            Exception baseException = de.GetBaseException();
            Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
        }
        catch (Exception e)
        {
            Console.WriteLine("Error: {0}", e);
        }
        finally
        {
            Console.WriteLine("End of program, press any key to exit.");
            Console.ReadKey();
        }
    }

    public async Task CosmosAsync()
    {
        // Create a new instance of the Cosmos Client
        this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey);

        // Runs the CreateDatabaseAsync method
        await this.CreateDatabaseAsync();

        // Run the CreateContainerAsync method
        await this.CreateContainerAsync();

        // Create items
        await this.CreateItemsAsync();

        // Query items
        await this.ReadAllItems();
    }

    private async Task CreateDatabaseAsync()
    {
        // Create a new database using the cosmosClient
        this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
        Console.WriteLine("Created Database: {0}\n", this.database.Id);
    }

    private async Task CreateContainerAsync()
    {
        // Create a new container
        this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, partitionKeyPath);
        Console.WriteLine("Created Container: {0}\n", this.container.Id);
    }

    private async Task CreateItemsAsync()
    {
        // Create some items to be read back later
        for (int i = 0; i < 5; i++)
        {
            SalesOrder salesOrder = GetSalesOrderSample($"SalesOrderForReadMany{i}");
            ItemResponse<SalesOrder> response = await this.container.CreateItemAsync(salesOrder, new PartitionKey(salesOrder.AccountNumber));
            Console.WriteLine("Created Item {0}\n", response.Resource);
        } 
    }

    // <ReadAllItems>
    private async Task ReadAllItems()
    {
        //******************************************************************************************************************
        // 1.3 - Read all items in a container
        //
        // NOTE: Operations like AsEnumerable(), ToList(), ToArray() will make as many trips to the database
        //       as required to fetch the entire result-set. Even if you set MaxItemCount to a smaller number. 
        //       MaxItemCount just controls how many results to fetch each trip. 
        //******************************************************************************************************************
        Console.WriteLine("\n1.3 - Read all items with query using a specific partition key");

        List<SalesOrder> allSalesForAccount1 = new List<SalesOrder>();
        using (FeedIterator<SalesOrder> resultSet = this.container.GetItemQueryIterator<SalesOrder>(
            queryDefinition: null,
            requestOptions: new QueryRequestOptions()
            {
                PartitionKey = new PartitionKey("Account1")
            }))
        {
            while (resultSet.HasMoreResults)
            {
                FeedResponse<SalesOrder> response = await resultSet.ReadNextAsync();
                SalesOrder sale = response.First();
                Console.WriteLine($"\n1.3.1 Account Number: {sale.AccountNumber}; Id: {sale.Id};");
                if (response.Diagnostics != null)
                {
                    Console.WriteLine($" Diagnostics {response.Diagnostics.ToString()}");
                }

                allSalesForAccount1.AddRange(response);
            }
        }
    }

    private static SalesOrder GetSalesOrderSample(string itemId)
    {
        SalesOrder salesOrder = new SalesOrder
        {
            Id = itemId,
            AccountNumber = "Account1",
            PurchaseOrderNumber = "PO18009186470",
            OrderDate = new DateTime(2005, 7, 1),
            SubTotal = 419.4589m,
            TaxAmount = 12.5838m,
            Freight = 472.3108m,
            TotalDue = 985.018m,
            Items = new SalesOrderDetail[]
            {
                new SalesOrderDetail
                {
                    OrderQty = 1,
                    ProductId = 760,
                    UnitPrice = 419.4589m,
                    LineTotal = 419.4589m
                }
            },
        };

        // Set the "ttl" property to auto-expire sales orders in 30 days 
        salesOrder.TimeToLive = 60 * 60 * 24 * 30;

        return salesOrder;
    }
}


