FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 9090

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore "Hermes.Cluster.Server/Hermes.Cluster.Server.csproj" --locked-mode
WORKDIR "/src/Hermes.Cluster.Server"
RUN dotnet build "Hermes.Cluster.Server.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Hermes.Cluster.Server.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Hermes.Cluster.Server.dll"]
