using System.Net;
using System.Net.Sockets;
using DotNext.Net.Cluster;
using DotNext.Net.Cluster.Consensus.Raft;
using DotNext.Net.Cluster.Consensus.Raft.Http;
using DotNext.Net.Cluster.Consensus.Raft.Membership;
using DotNext.Net.Http;
using Hermes.Cluster.Server.Bootstrap;
using Microsoft.AspNetCore.HttpOverrides;
using Microsoft.Extensions.Options;


var builder = WebApplication.CreateBuilder(args);

builder.Services.AddOptions();
builder.Services.AddMvc().AddControllersAsServices();
builder.Services.Configure<DnsBootstrapConfiguration>(builder.Configuration);
builder.Services.AddHostedService<PeerMaintenanceBackgroundService>();


// Get network information needed for startup
var listenNetworks =
    SystemHelper.DetermineIpNetworks(
        builder.Configuration.GetValue<string[]>("ListenNetworks", Array.Empty<string>()));
var clusterNetwork =
    (SystemHelper.GetIpNetworks(builder.Configuration.GetValue("ClusterNetwork", string.Empty)) ??
     Array.Empty<IPNetwork>()).SingleOrDefault();
if (clusterNetwork == null)
{
    clusterNetwork = listenNetworks.OrderByDescending(i => i.PrefixLength)
                         .FirstOrDefault(i => i.Prefix.AddressFamily == AddressFamily.InterNetwork) ??
                     listenNetworks.OrderByDescending(i => i.PrefixLength).FirstOrDefault(i =>
                         i.Prefix.AddressFamily == AddressFamily.InterNetworkV6);
    Console.WriteLine(
        $"Choosing network {clusterNetwork.Prefix}/{clusterNetwork.PrefixLength} because a network was not configured.");
}

var clusterIpAddress = IPAddress.None;
List<string> urls = new List<string>();
foreach (var network in listenNetworks.SelectMany(SystemHelper.GetHostNetworkAddressInformation))
{
    urls.Add(new Uri($"http://{network.Address}:{SystemHelper.ClusterPort}/").ToString());
    Console.WriteLine($"Listening on {urls.Last()}");
    if (clusterNetwork.Contains(network.Address))
    {
        clusterIpAddress = network.Address;
    }
}

if (urls.Count == 0) throw new InvalidOperationException("Unable to configure listen addresses");
if (clusterIpAddress.Equals(IPAddress.Any))
    throw new InvalidOperationException("Unable to determine local node IP Address");

Console.WriteLine($"Our address is: {clusterIpAddress}");

var discoveryDns = builder.Configuration.GetValue<string>("DiscoveryDns");
List<HttpEndPoint> initialPeers = new List<HttpEndPoint>();
if (!string.IsNullOrWhiteSpace(discoveryDns))
{
    Console.WriteLine("Discovery DNS provided, starting discovery process");
    var results = await DnsBootstrapper.BootstrapFromAsync(discoveryDns, clusterIpAddress);
    initialPeers.AddRange(results);

    Console.WriteLine("Bootstrap complete");
    Console.WriteLine($"Got {initialPeers.Count} peer(s) from discovery");
    foreach (var peer in initialPeers)
    {
        Console.WriteLine($"-- {peer}");
    }
}

builder.Services.Configure<HttpClusterMemberConfiguration>(config =>
{
    config.PublicEndPoint = new HttpEndPoint(clusterIpAddress, SystemHelper.ClusterPort, false);
    config.ColdStart = false;
});

builder.Services.UseInMemoryConfigurationStorage(endPoints =>
{
    foreach (var peer in initialPeers)
    {
        var peerId = ClusterMemberId.FromEndPoint(peer);
        endPoints.Add(peerId, peer);
    }
});

builder.JoinCluster();
var app = builder.Build();
app.Urls.Clear();
foreach (var url in urls)
{
    app.Urls.Add(url);
}

app.UseConsensusProtocolHandler();
app.UseRouting()
    .UseEndpoints(config => { config.MapControllers(); });

await app.RunAsync();

public class DnsBootstrapConfiguration
{
    public string DiscoveryDns { get; set; }
}


public class PeerMaintenanceBackgroundService : BackgroundService
{
    private readonly IClusterConfigurationStorage<HttpEndPoint> _configurationStorage;
    private readonly IOptionsMonitor<DnsBootstrapConfiguration> _optionsMonitor;
    private readonly ILogger<PeerMaintenanceBackgroundService> _logger;

    public PeerMaintenanceBackgroundService(IClusterConfigurationStorage<HttpEndPoint> configurationStorage,
        IOptionsMonitor<DnsBootstrapConfiguration> optionsMonitor,
        ILogger<PeerMaintenanceBackgroundService> logger)
    {
        _configurationStorage = configurationStorage;
        _optionsMonitor = optionsMonitor;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var addresses = new HashSet<IPAddress>();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var cancellationTokenSource = CancellationTokenSource.CreateLinkedTokenSource(stoppingToken);
                var cancellationToken = cancellationTokenSource.Token;

                await Task.Delay(TimeSpan.FromSeconds(10), cancellationToken);

                var config = _optionsMonitor.CurrentValue;
                if (string.IsNullOrWhiteSpace(config.DiscoveryDns))
                {
                    await Task.Delay(TimeSpan.FromMinutes(5), cancellationToken);
                    continue;
                }

                var hosts = await Dns.GetHostAddressesAsync(config.DiscoveryDns, cancellationToken);
                if (hosts.Length == 0)
                {
                    await Task.Delay(5000, cancellationToken);
                    continue;
                }

                foreach (var host in hosts)
                {
                    if (await OnHostDetectedAsync(host, cancellationToken))
                        addresses.Add(host);
                }

                var goneHosts = addresses.Except(hosts).ToList();
                foreach (var host in goneHosts)
                {
                    if (await OnHostGoneAsync(host, cancellationToken)) 
                        addresses.Remove(host);
                }

                // We don't really need to know if we made changes.
                // apply is a no-op if there is no proposed configuration.
                await _configurationStorage.ApplyAsync(cancellationToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch
            {
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            }
        }
    }

    private async Task<bool> OnHostGoneAsync(IPAddress host, CancellationToken cancellationToken)
    {
        var httpEndPoint = new HttpEndPoint(host, SystemHelper.ClusterPort, false);
        var clusterMemberId = ClusterMemberId.FromEndPoint(httpEndPoint);

        var result = await _configurationStorage.RemoveMemberAsync(clusterMemberId, cancellationToken);
        if (result)
            _logger.LogInformation("New cluster member {endPoint} added to peers with id {id}", httpEndPoint,
                clusterMemberId);
        return result;
    }

    private async Task<bool> OnHostDetectedAsync(IPAddress host, CancellationToken cancellationToken)
    {
        var httpEndPoint = new HttpEndPoint(host, SystemHelper.ClusterPort, false);
        var clusterMemberId = ClusterMemberId.FromEndPoint(httpEndPoint);
        var result = await _configurationStorage.AddMemberAsync(clusterMemberId, httpEndPoint, cancellationToken);
        if (result)
            _logger.LogInformation(
                "Cluster member {endPoint} with id {id} was removed from the member list because it is no longer listed in the discovery DNS",
                httpEndPoint, clusterMemberId);

        return result;
    }
}