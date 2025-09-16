public virtual Task<SendMessageResponse> SendMessageAsync(SendMessageRequest request, System.Threading.CancellationToken cancellationToken = default(CancellationToken))
{
    var options = new InvokeOptions();
    options.RequestMarshaller = SendMessageRequestMarshaller.Instance;
    options.ResponseUnmarshaller = SendMessageResponseUnmarshaller.Instance;

    return InvokeAsync<SendMessageResponse>(request, options, cancellationToken);
}