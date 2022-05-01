using System.Diagnostics;
using System.Net;
using DotNext.Net.Cluster;
using DotNext.Net.Http;

namespace Hermes.Journal.Server.Bootstrap;

public class DnsBootstrapper
{
    public record Peer(string Host, int Port, bool IsSecure, bool IsLeader = false)
    {
        public HttpEndPoint ToHttpEndPoint()
        {
            return new HttpEndPoint(Host, Port, IsSecure);
        }
    }


    public static async Task<IEnumerable<HttpEndPoint>> BootstrapFromAsync(string discoveryDns,
        IPAddress localNodeIpAddress)
    {
        var cancellationTokenSource = new CancellationTokenSource();
        int sameCount = 0;
        IPAddress[] hosts = Array.Empty<IPAddress>();
        while (true)
        {
            var originalHosts = hosts;
            hosts = await Dns.GetHostAddressesAsync(discoveryDns);
            if (hosts.Length == 0)
            {
                await Task.Delay(5000);
                continue;
            }

            hosts = hosts
                .OrderBy(i => i.ToString())
                .ToArray();


            if (!cancellationTokenSource.TryReset())
            {
                cancellationTokenSource = new CancellationTokenSource();
            }

            cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(3));


            var cancellationToken = cancellationTokenSource.Token;
            var hostIdentificationTasks = hosts.Select(i => GetPeerStatus(i, cancellationToken));
            var results = await Task.WhenAll(hostIdentificationTasks);
            results = results.Where(i => i != null).ToArray();
            if (results.Any(i => i!.IsLeader))
            {
                Console.WriteLine("Found a cluster leader, returning peer list, bootstrap done.");
                return hosts.Select(i => new HttpEndPoint(i, SystemHelper.ClusterPort, false));
            }

            if (hosts.SequenceEqual(originalHosts))
            {
                sameCount++;

                if (sameCount >= 10)
                {
                    return hosts.Select(i => new HttpEndPoint(i, SystemHelper.ClusterPort, false)).ToArray();
                }
            }
            else
            {
                sameCount = 0;
                Console.WriteLine("Bootstrap peers changed, resetting counter to 0");
            }

            Console.WriteLine("No leader available yet, waiting for leader before allowing bootstrap");
            // ReSharper disable once MethodSupportsCancellation
            await Task.Delay(TimeSpan.FromSeconds(2));
        }
    }

    private static HttpClient _httpClient = new HttpClient(new SocketsHttpHandler()
    {
        MaxConnectionsPerServer = 32
    })
    {
        Timeout = TimeSpan.FromSeconds(1)
    };

    private const string BasePath = "/api/clusterState/";
    private const string IdentityApi = BasePath + "identity";
    private const string LeaderApi = BasePath + "leader";

    private static async Task<Peer?> GetPeerStatus(IPAddress address, CancellationToken cancellationToken)
    {
        var uriBuilder = new HttpEndPoint(address, SystemHelper.ClusterPort, false).CreateUriBuilder();
        uriBuilder.Path = LeaderApi;

        try
        {
            var leaderTask = _httpClient.GetAsync(uriBuilder.Uri, cancellationToken);
            uriBuilder.Path = IdentityApi;
            var peer = await _httpClient.GetFromJsonAsync<Peer>(uriBuilder.Uri, cancellationToken)
                .ConfigureAwait(false);
            Debug.Assert(peer != null, nameof(peer) + " != null");
            var leaderResponse = await leaderTask.ConfigureAwait(false);
            if (leaderResponse.IsSuccessStatusCode)
            {
                var leaderPeer =
                    await leaderResponse.Content.ReadFromJsonAsync<Peer>(cancellationToken: cancellationToken);
                Debug.Assert(leaderPeer != null, nameof(leaderPeer) + " != null");

                if (leaderPeer.Host == peer.Host && leaderPeer.Port == peer.Port &&
                    leaderPeer.IsSecure == peer.IsSecure)
                    return leaderPeer with {IsLeader = true};
            }

            return peer;
        }
        catch
        {
            return null;
        }
    }
}