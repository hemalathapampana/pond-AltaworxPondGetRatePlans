using Altaworx.AWS.Core;
using Altaworx.AWS.Core.Models;
using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Amazon.SQS.Model;
using Amazon.SQS;
using Amop.Core.Constants;
using Amop.Core.Helpers;
using Amop.Core.Helpers.Pond;
using Amop.Core.Models;
using Amop.Core.Models.Pond;
using Amop.Core.Repositories.Environment;
using Amop.Core.Repositories.Pond;
using Amop.Core.Services.Http;
using Amop.Core.Services.Pond;
using Microsoft.Data.SqlClient;
using Polly;
using System.Data;
using Amazon;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace AltaworxPondGetRatePlans;

public class Function : AwsFunctionBase
{
    private int PageSize;
    private int MaxPagesPerLambdaInstance;
    private string? GetRatePlansQueueURL;
    private string? PondDeviceCarrierRatePlanQueueURL;
    private string pondDeviceCleanUpQueueURL;
    private string? PondGetRatePlansEndpoint;
    private readonly EnvironmentRepository environmentRepo = new EnvironmentRepository();
    private PondRepository pondRepository;

    public async Task FunctionHandler(SQSEvent sqsEvent, ILambdaContext context)
    {
        AmopLambdaContext? lambdaContext = null;
        try
        {
            lambdaContext = BaseAmopFunctionHandler(context);
            ArgumentNullException.ThrowIfNull(lambdaContext);

            pondRepository = new PondRepository(lambdaContext.CentralDbConnectionString);

            TryGetAllEnvironmentVariables(lambdaContext);

            await ProcessEventAsync(lambdaContext, sqsEvent);
        }
        catch (Exception ex)
        {
            if (lambdaContext == null)
            {
                context.Logger.Log(CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
            else
            {
                LogInfo(lambdaContext, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
            }
        }
        base.CleanUp(lambdaContext);
    }

    private void TryGetAllEnvironmentVariables(AmopLambdaContext lambdaContext)
    {
        // Lambda related configurations
        GetRatePlansQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, PondHelper.CommonString.POND_RATE_PLANS_QUEUE_URL_VARIABLE_KEY);
        PondDeviceCarrierRatePlanQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, PondHelper.CommonString.POND_DEVICE_CARRIER_RATE_PLANS_QUEUE_URL_VARIABLE_KEY);
        MaxPagesPerLambdaInstance = GetIntValueFromEnvironmentVariable(lambdaContext, environmentRepo, PondHelper.CommonString.MAX_PAGES_PER_INSTANCE_VARIABLE_KEY, PondHelper.CommonConfig.DEFAULT_MAX_PAGES_PER_LAMBDA_INSTANCE);
        pondDeviceCleanUpQueueURL = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, PondHelper.CommonString.CLEAN_UP_QUEUE_URL_VARIABLE_KEY);
        // API related configurations
        PondGetRatePlansEndpoint = GetStringValueFromEnvironmentVariable(lambdaContext.Context, environmentRepo, PondHelper.CommonString.POND_GET_RATE_PLANS_END_POINT_VARIABLE_KEY);
        // Sync logic related configurations
        PageSize = GetIntValueFromEnvironmentVariable(lambdaContext, environmentRepo, PondHelper.CommonString.PAGE_SIZE, PondHelper.CommonConfig.DEFAULT_PAGE_SIZE);
    }

    private async Task ProcessEventAsync(AmopLambdaContext context, SQSEvent sqsEvent)
    {
        LogInfo(context, CommonConstants.SUB);
        if (sqsEvent?.Records != null && sqsEvent?.Records?.Count != 0)
        {
            var processedRecordCount = sqsEvent.Records.Count;
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.BEGINNING_PROCESS, processedRecordCount));
            foreach (var record in sqsEvent.Records)
            {
                LogInfo(context, CommonConstants.INFO, $"MessageId: {record.MessageId}");
                var sqsValues = new SqsValues(context, record);

                if (sqsValues.SyncAction == PondSyncAction.SyncFromAPIToStaging)
                {
                    // Run for the current service provider id (specified in the SQS Message)
                    await TryProcessSyncByServiceProviderId(context, sqsValues);
                }
                else
                {
                    // Run once to load all data from staging to main tables
                    await TryProcessLoadData(context, sqsValues);
                }
            }
        }
        else
        {
            var sqsValues = new SqsValues();
            await TryProcessSyncByServiceProviderId(context, sqsValues);
        }
    }

    private async Task TryProcessLoadData(AmopLambdaContext context, SqsValues sqsValues)
    {
        try
        {
            var errorMessages = new List<string>();
            var sqlRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
            sqlRetryPolicy.Execute(() => pondRepository.LoadRatePlanFromStagingTable(ParameterizedLog(context), sqsValues.ServiceProviderId, context.Context.FunctionName));
            // Pond Device Clean up processing
            await SendMessageToJasperDeviceCleanUpQueue(context, pondDeviceCleanUpQueueURL, sqsValues.ServiceProviderId);
            // Check if there is another service provider
            await CheckNextPondServiceProviderAsync(context, sqsValues.ServiceProviderId);
        }
        catch (Exception ex)
        {
            LogInfo(context, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
        }
    }

    private async Task TryProcessSyncByServiceProviderId(AmopLambdaContext context, SqsValues sqsValues)
    {
        try
        {
            var errorMessages = new List<string>();
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
            if (sqsValues.ServiceProviderId == 0)
            {
                var serviceProviderId = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.Pond, sqsValues.ServiceProviderId);
                if (serviceProviderId > 0)
                {
                    sqsValues.ServiceProviderId = serviceProviderId;
                }
                else
                {
                    LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.NO_SERVICE_PROVIDER_FOUND, CommonConstants.POND_CARRIER_NAME));
                    return;
                }
                var nextInventoryId = pondRepository.GetNextInventoryId(ParameterizedLog(context), serviceProviderId, 0);
                if (nextInventoryId > 0)
                {
                    sqsValues.InventoryId = nextInventoryId;
                }
                else
                {
                    LogInfo(context, CommonConstants.INFO, LogCommonStrings.POND_INVENTORY_EMPTY);
                    return;
                }
                // Truncate staging tables
                sqlTransientRetryPolicy.Execute(() => pondRepository.TruncateStagingTables(ParameterizedLog(context)));
            }

            var pondAuth = pondRepository.GetPondAuthentication(ParameterizedLog(context), context.Base64Service, sqsValues.ServiceProviderId);
            if (pondAuth == null)
            {
                LogInfo(context, CommonConstants.ERROR, string.Format(LogCommonStrings.SERVICE_PROVIDER_NO_AUTH_INFO, sqsValues.ServiceProviderId));
                return;
            }

            var httpRequestFactory = new HttpRequestFactory();
            var pondAPIService = new PondApiService(pondAuth, httpRequestFactory, context.IsProduction);

            // Sync Packages
            await SyncPackageTypes(context, sqsValues, sqlTransientRetryPolicy, pondAPIService);
        }
        catch (Exception ex)
        {
            LogInfo(context, CommonConstants.EXCEPTION, ex.Message + " " + ex.StackTrace);
        }
    }

    public async Task SyncPackageTypes(AmopLambdaContext context, SqsValues sqsValues, ISyncPolicy syncPolicy, PondApiService pondApiService)
    {
        var formattedEndpoint = string.Format(PondGetRatePlansEndpoint, sqsValues.InventoryId);
        await pondApiService.GetListFromPondAPIAsync<PondPackageType, PondPackageTypeListResponse>(
            context.logger, MaxPagesPerLambdaInstance, syncPolicy, sqsValues.PageNumber, PageSize,
            (offset, pageSize) => pondApiService.GetPondListAsync<PondPackageTypeListResponse>(HttpClientSingleton.Instance, formattedEndpoint, offset, pageSize),
            (response) => response.IsLastPage,
            (response) => response.Elements,
            (response) => LoadPackageTypesToStagingTable(context, response, sqsValues.ServiceProviderId, sqsValues.InventoryId),
            async (pageNumber, isLastPage) => await CheckSyncPackageTypeProgress(context, sqsValues.ServiceProviderId, pageNumber, isLastPage, sqsValues.InventoryId),
            context.Context);
    }

    public static void LoadPackageTypesToStagingTable(AmopLambdaContext context, List<PondPackageType> pondPackageTypes, int serviceProviderId, int inventoryId)
    {
        LogInfo(context, CommonConstants.SUB);
        var pondPackageTypeTable = new DataTable();
        pondPackageTypeTable.Columns.Add(CommonColumnNames.RatePlanId, typeof(long));
        pondPackageTypeTable.Columns.Add(CommonColumnNames.RatePlanName, typeof(string));
        pondPackageTypeTable.Columns.Add(CommonColumnNames.ServiceProviderId, typeof(int));
        pondPackageTypeTable.Columns.Add(CommonColumnNames.Status, typeof(string));
        pondPackageTypeTable.Columns.Add(CommonColumnNames.PondInventoryId, typeof(int));
        pondPackageTypeTable.Columns.Add(CommonColumnNames.DataUsageAllowanceInBytes, typeof(long));
        pondPackageTypeTable.Columns.Add(CommonColumnNames.CreatedBy, typeof(string));
        pondPackageTypeTable.Columns.Add(CommonColumnNames.CreatedDate, typeof(string));

        foreach (var pondPackageTypeItem in pondPackageTypes)
        {
            var pondPackageTypeRow = pondPackageTypeTable.NewRow();
            pondPackageTypeRow[CommonColumnNames.RatePlanId] = pondPackageTypeItem.PackageTypeId;
            pondPackageTypeRow[CommonColumnNames.RatePlanName] = pondPackageTypeItem.Name;
            pondPackageTypeRow[CommonColumnNames.ServiceProviderId] = serviceProviderId;
            pondPackageTypeRow[CommonColumnNames.Status] = pondPackageTypeItem.Status;
            pondPackageTypeRow[CommonColumnNames.PondInventoryId] = inventoryId;
            pondPackageTypeRow[CommonColumnNames.DataUsageAllowanceInBytes] = pondPackageTypeItem.DataUsageAllowanceInBytes;
            pondPackageTypeRow[CommonColumnNames.CreatedBy] = context.Context.FunctionName;
            pondPackageTypeRow[CommonColumnNames.CreatedDate] = DateTime.UtcNow;
            pondPackageTypeTable.Rows.Add(pondPackageTypeRow);
        }

        List<SqlBulkCopyColumnMapping> columnMappings = SQLBulkCopyHelper.AutoMapColumns(pondPackageTypeTable);
        SqlBulkCopy(context, context.CentralDbConnectionString, pondPackageTypeTable, DatabaseTableNames.PondCarrierRatePlanStaging, columnMappings);
    }

    private async Task CheckSyncPackageTypeProgress(AmopLambdaContext context, int serviceProviderId, int currentPage, bool isLastPage, int inventoryId)
    {
        // Check if final page of current inventory Id
        if (!isLastPage)
        {
            await BuildSQSMessage(context, serviceProviderId, currentPage, inventoryId);
        }
        else
        {
            // Try getting next inventory Id (should be from staging table?)
            var nextInventoryId = pondRepository.GetNextInventoryId(ParameterizedLog(context), serviceProviderId, inventoryId);
            if (nextInventoryId > 0)
            {
                // Need to queue up new instance for the same step
                await BuildSQSMessage(context, serviceProviderId, PondHelper.CommonConfig.STARTING_PAGE_NUMBER, nextInventoryId);
            }
            else
            {
                LogInfo(context, CommonConstants.INFO, LogCommonStrings.POND_INVENTORY_EMPTY);
                await BuildSQSMessage(context, serviceProviderId, currentPage, syncAction: PondSyncAction.LoadFromStaging);
            }
        }
    }

    private async Task CheckNextPondServiceProviderAsync(AmopLambdaContext context, int currentServiceProviderId)
    {
        LogInfo(context, CommonConstants.SUB);
        var nextServiceProviderId = ServiceProviderCommon.GetNextServiceProviderId(context.CentralDbConnectionString, IntegrationType.Pond, currentServiceProviderId);
        if (nextServiceProviderId > 0)
        {
            var nextInventoryId = pondRepository.GetNextInventoryId(ParameterizedLog(context), nextServiceProviderId, 0);
            if (nextInventoryId <= 0)
            {
                LogInfo(context, CommonConstants.INFO, LogCommonStrings.POND_INVENTORY_EMPTY);
                return;
            }
            var errorMessages = new List<string>();
            // Truncate staging tables
            var sqlTransientRetryPolicy = RetryPolicyHelper.GetSqlTransientPolicy(context.logger, errorMessages);
            sqlTransientRetryPolicy.Execute(() => pondRepository.TruncateStagingTables(ParameterizedLog(context)));
            await BuildSQSMessage(context, nextServiceProviderId, PondHelper.CommonConfig.STARTING_PAGE_NUMBER, nextInventoryId, PondSyncAction.SyncFromAPIToStaging);
        }
        else
        {
            LogInfo(context, CommonConstants.INFO, LogCommonStrings.NOT_HAVE_NEXT_POND_SERVICE_PROVIDER_ID);
            await SendMessageToPondDeviceCarrierRatePlanQueue(context, PondDeviceCarrierRatePlanQueueURL);
        }
    }

    private async Task BuildSQSMessage(AmopLambdaContext context, int serviceProviderId, int currentPage = 0, int inventoryId = 0, PondSyncAction syncAction = PondSyncAction.SyncFromAPIToStaging)
    {
        LogInfo(context, CommonConstants.SUB, $"{currentPage}, {inventoryId}, {syncAction}");
        if (string.IsNullOrWhiteSpace(GetRatePlansQueueURL))
        {
            LogInfo(context, CommonConstants.EXCEPTION, string.Format(LogCommonStrings.ENVIRONMENT_VARIABLE_NOT_CONFIGURED, PondHelper.CommonString.POND_RATE_PLANS_QUEUE_URL_VARIABLE_KEY));
            return;
        }

        using (var client = new AmazonSQSClient(AwsCredentials(context), RegionEndpoint.USEast1))
        {
            var request = new SendMessageRequest
            {
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                {
                    {SQSMessageKeyConstant.SERVICE_PROVIDER_ID, new MessageAttributeValue {DataType = nameof(String), StringValue = serviceProviderId.ToString()}},
                    {SQSMessageKeyConstant.SYNC_ACTION, new MessageAttributeValue {DataType = nameof(String), StringValue = ((int)syncAction).ToString()}},
                    {SQSMessageKeyConstant.PAGE_NUMBER, new MessageAttributeValue {DataType = nameof(String), StringValue = currentPage.ToString()}},
                    {SQSMessageKeyConstant.INVENTORY_ID, new MessageAttributeValue {DataType = nameof(String), StringValue = inventoryId.ToString()}},
                },
                MessageBody = LogCommonStrings.SENDING_SQS_MESSAGE_TO_POND_GET_RATE_PLAN_LAMBDA,
                QueueUrl = GetRatePlansQueueURL,
            };
            LogInfo(context, CommonConstants.INFO, request.MessageBody);

            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, $"{response.HttpStatusCode:d} {response.HttpStatusCode:g}");
        }
    }

    private async Task SendMessageToPondDeviceCarrierRatePlanQueue(KeySysLambdaContext context, string queueURL)
    {
        LogInfo(context, CommonConstants.SUB, $"({queueURL})");
        var awsCredentials = AwsCredentials(context);
        using (var client = new AmazonSQSClient(awsCredentials, RegionEndpoint.USEast1))
        {
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, queueURL));

            var request = new SendMessageRequest
            {
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.INITIALIZE_PROCESSING, new MessageAttributeValue { DataType = nameof(String), StringValue = true.ToString()}},
                    },
                MessageBody = string.Format(LogCommonStrings.PROCESSED_CARRIER_RATE_PLANS, PondHelper.CommonString.CARRIER_NAME),
                QueueUrl = queueURL
            };

            LogInfo(context, CommonConstants.INFO, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);
            LogInfo(context, CommonConstants.INFO, $"{CommonConstants.MESSAGE_BODY}: {request.MessageBody}");

            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, response.HttpStatusCode);
        }
    }

    private async Task SendMessageToJasperDeviceCleanUpQueue(KeySysLambdaContext context, string? cleanUpQueueURL, int serviceProviderId)
    {
        LogInfo(context, CommonConstants.SUB, $"{cleanUpQueueURL}, {serviceProviderId}");
        if (string.IsNullOrEmpty(cleanUpQueueURL))
        {
            return;
        }
        var retryCount = 0;
        var awsCredentials = AwsCredentials(context);
        using (var client = new AmazonSQSClient(awsCredentials, Amazon.RegionEndpoint.USEast1))
        {
            LogInfo(context, CommonConstants.INFO, string.Format(LogCommonStrings.SENDING_MESSAGE_TO_URL, cleanUpQueueURL));
            var request = new SendMessageRequest
            {
                MessageAttributes = new Dictionary<string, MessageAttributeValue>
                    {
                        { SQSMessageKeyConstant.RETRY_COUNT, new MessageAttributeValue { DataType = nameof(String), StringValue = retryCount.ToString() } },
                        { SQSMessageKeyConstant.INTEGRATION_TYPE, new MessageAttributeValue { DataType = nameof(String), StringValue = ((int)IntegrationType.Pond).ToString() } },
                        { SQSMessageKeyConstant.SERVICE_PROVIDER_ID, new MessageAttributeValue { DataType = nameof(String), StringValue = serviceProviderId.ToString() } }
                    },
                MessageBody = LogCommonStrings.END_PROCESS_GET_DEVICES,
                QueueUrl = cleanUpQueueURL
            };
            LogInfo(context, CommonConstants.INFO, LogCommonStrings.SEND_MESSAGE_REQUEST_READY);
            var response = await client.SendMessageAsync(request);
            LogInfo(context, CommonConstants.INFO, response.HttpStatusCode);
        }
    }
}
