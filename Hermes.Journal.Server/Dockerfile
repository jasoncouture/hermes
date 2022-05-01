FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app
EXPOSE 9090

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY . .
RUN dotnet restore "Hermes.Journal.Server/Hermes.Journal.Server.csproj"
WORKDIR "/src/Hermes.Journal.Server"
RUN dotnet build "Hermes.Journal.Server.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "Hermes.Journal.Server.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "Hermes.Journal.Server.dll"]
