using System.Text.RegularExpressions;
using DotNext.Net.Cluster.Discovery.HyParView.Http;
using DotNext.Net.Http;

void EarlyInfo(string message) => Console.WriteLine("EARLY/I: {0}", message);
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions();
builder.JoinMesh();

    builder.Services.Configure<HttpPeerConfiguration>(config =>
    {
        if (!builder.Configuration.GetValue<bool>("KubernetesIntegration", true))
        {
            EarlyInfo("Not detecting stateful set mode, as kubernetes integration is disabled.");
            return;
        }
        EarlyInfo("Detecting kubernetes...");
        if (string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("POD_NAME")))
        {
            EarlyInfo("Not running in kubernetes. Continuing with existing configuration.");
            return;
        }

        var regex = new Regex(@"^(.*?)-\d{1,2}$", RegexOptions.Singleline);
        if (!regex.IsMatch(Environment.MachineName))
        {
            EarlyInfo("Doesn't look like we're in a stateful set. Can't auto-configure bootstrap node.");
            EarlyInfo($"Hostname: `{Environment.MachineName}` does not end with a dash (-) followed by 1-2 numbers");
            EarlyInfo("Aborting Kubernetes auto-configuration.");
            return;
        }
        EarlyInfo("Kubernetes detected, updating configuration.");
        var port = Environment.GetEnvironmentVariable("SERVICE_PORT") ?? "8080";
        config.LocalNode = new HttpEndPoint(new Uri($"http://{Environment.MachineName}:{port}"));
        EarlyInfo($"Local node: {config.LocalNode}");
        if (!Environment.MachineName.EndsWith("-0"))
        {
            EarlyInfo("Not running in stateful set 0, setting contact node to -0");
            var target = Regex.Replace(Environment.MachineName, @"^(.*?)-\d{1,2}$", "$1-0");
            config.ContactNode = new HttpEndPoint(new Uri($"http://{target}:{port}"));
        }
        EarlyInfo($"Contact node: {config.ContactNode}");
    });

var app = builder.Build();

app.UseHyParViewProtocolHandler();

app.Run();
