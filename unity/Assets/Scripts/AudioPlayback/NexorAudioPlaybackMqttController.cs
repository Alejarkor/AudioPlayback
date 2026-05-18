using UnityEngine;
using System;
using System.Threading.Tasks;
using System.Collections.Concurrent;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Protocol;

public class NexorAudioPlaybackMqttController : MonoBehaviour
{
    [Header("MQTT")]
    public string brokerHost = "192.168.28.92";
    public int brokerPort = 1883;
    public string mqttUser = "";
    public string mqttPassword = "";
    public bool autoConnectOnStart = true;
    public bool autoReconnect = true;
    public float reconnectIntervalSeconds = 3f;

    [Header("Nexor")]
    public string mqttNamespace = "nexor/v1";
    public string nodeId = "nexor-01";
    public string serviceName = "audio_playback";

    [Header("Sender local")]
    public AudioSenderUdp sender;
    public bool autoApplyEndpointToSender = true;
    public bool autoStartSenderWhenEndpointArrives = false;

    [Header("Config deseada")]
    public float desiredVolume = 1.0f;

    [Header("Comandos one-shot desde Inspector")]
    public bool requestConnect;
    public bool requestDisconnect;
    public bool requestGetState;
    public bool requestRestart;
    public bool requestStandby;
    public bool requestResume;
    public bool requestApplyVolume;
    public bool requestStartSender;
    public bool requestStopSender;

    [Header("Estado runtime")]
    public bool isConnected;
    public bool isConnecting;
    public string lastStateJson;
    public string lastConfigJson;
    public string lastEndpointJson;
    public string lastEventJson;
    public string lastCapabilitiesJson;
    public string lastError;

    [Header("Estado parseado")]
    public string currentStatus;
    public bool currentHealthy;
    public string currentTransport;
    public float currentVolume;
    public string endpointHost;
    public int endpointPort;
    public int endpointSampleRate;
    public int endpointChannels;
    public int endpointBitDepth;
    public string endpointOutputDevice;
    public int currentPid;

    private IMqttClient client;
    private readonly ConcurrentQueue<Action> mainThreadQueue = new ConcurrentQueue<Action>();
    private float nextReconnectAt = 0f;

    private string BaseTopic => $"{mqttNamespace}/nodes/{nodeId}/services/{serviceName}";
    private string TopicCmd => $"{BaseTopic}/cmd";
    private string TopicState => $"{BaseTopic}/state";
    private string TopicConfigReported => $"{BaseTopic}/config/reported";
    private string TopicConfigDesired => $"{BaseTopic}/config/desired";
    private string TopicEndpoint => $"{BaseTopic}/endpoint";
    private string TopicEvents => $"{BaseTopic}/events";
    private string TopicCapabilities => $"{BaseTopic}/capabilities";

    [Serializable]
    private class ServiceStateMsg
    {
        public string service;
        public string status;
        public bool healthy;
        public string transport;
        public float volume;
        public int pid;
        public int uptime_s;
        public string ts;
        public string last_error;
    }

    [Serializable]
    private class EndpointMsg
    {
        public string service;
        public string transport;
        public string direction;
        public string host;
        public int port;
        public int sample_rate;
        public int channels;
        public int bit_depth;
        public string output_device;
        public int pid;
        public string ts;
    }

    [Serializable]
    private class ConfigDesiredEnvelope
    {
        public ConfigPayload config;
    }

    [Serializable]
    private class ConfigPayload
    {
        public float volume;
    }

    void Start()
    {
        Application.runInBackground = true;
        EnsureClient();

        if (autoConnectOnStart)
            _ = ConnectAsync();
    }

    void Update()
    {
        while (mainThreadQueue.TryDequeue(out var action))
            action?.Invoke();

        ProcessInspectorRequests();

        if (autoReconnect && !isConnected && !isConnecting && Time.unscaledTime >= nextReconnectAt)
            _ = ConnectAsync();
    }

    private void EnsureClient()
    {
        if (client != null)
            return;

        var factory = new MqttFactory();
        client = factory.CreateMqttClient();

        client.ApplicationMessageReceivedAsync += e =>
        {
            string topic = e.ApplicationMessage?.Topic ?? "";
            string json = e.ApplicationMessage?.ConvertPayloadToString() ?? "";

            mainThreadQueue.Enqueue(() =>
            {
                try
                {
                    HandleIncomingMessage(topic, json);
                }
                catch (Exception ex)
                {
                    lastError = ex.Message;
                    Debug.LogWarning($"[NexorAudioPlaybackMqttController] Error parseando topic {topic}: {ex}");
                }
            });

            return Task.CompletedTask;
        };

        client.ConnectedAsync += e =>
        {
            mainThreadQueue.Enqueue(() =>
            {
                isConnected = true;
                isConnecting = false;
                lastError = "";
                Debug.Log("[NexorAudioPlaybackMqttController] MQTT conectado");
            });

            return Task.CompletedTask;
        };

        client.DisconnectedAsync += e =>
        {
            mainThreadQueue.Enqueue(() =>
            {
                isConnected = false;
                isConnecting = false;
                nextReconnectAt = Time.unscaledTime + reconnectIntervalSeconds;
                Debug.LogWarning("[NexorAudioPlaybackMqttController] MQTT desconectado");
            });

            return Task.CompletedTask;
        };
    }

    private void HandleIncomingMessage(string topic, string json)
    {
        if (topic == TopicState)
        {
            lastStateJson = json;
            var msg = JsonUtility.FromJson<ServiceStateMsg>(json);
            if (msg != null)
            {
                currentStatus = msg.status;
                currentHealthy = msg.healthy;
                currentTransport = msg.transport;
                currentVolume = msg.volume;
                currentPid = msg.pid;
            }
        }
        else if (topic == TopicConfigReported)
        {
            lastConfigJson = json;
        }
        else if (topic == TopicEndpoint)
        {
            lastEndpointJson = json;
            var msg = JsonUtility.FromJson<EndpointMsg>(json);
            if (msg != null)
            {
                endpointHost = msg.host;
                endpointPort = msg.port;
                endpointSampleRate = msg.sample_rate;
                endpointChannels = msg.channels;
                endpointBitDepth = msg.bit_depth;
                endpointOutputDevice = msg.output_device;
                currentTransport = msg.transport;
                currentPid = msg.pid;

                if (autoApplyEndpointToSender && sender != null)
                {
                    sender.SetRemoteEndpoint(endpointHost, endpointPort);
                    sender.sampleRate = endpointSampleRate;
                    sender.channels = endpointChannels;
                    sender.is16Bits = endpointBitDepth != 24;

                    if (autoStartSenderWhenEndpointArrives)
                    {
                        sender.StartMicrophone();
                        sender.StartSending();
                    }
                }
            }
        }
        else if (topic == TopicEvents)
        {
            lastEventJson = json;
        }
        else if (topic == TopicCapabilities)
        {
            lastCapabilitiesJson = json;
        }
    }

    private void ProcessInspectorRequests()
    {
        ConsumeFlag(ref requestConnect, () => _ = ConnectAsync());
        ConsumeFlag(ref requestDisconnect, () => _ = DisconnectAsync());
        ConsumeFlag(ref requestGetState, () => _ = SendGetStateAsync());
        ConsumeFlag(ref requestRestart, () => _ = SendRestartAsync());
        ConsumeFlag(ref requestStandby, () => _ = SendStandbyAsync());
        ConsumeFlag(ref requestResume, () => _ = SendResumeAsync());
        ConsumeFlag(ref requestApplyVolume, () => _ = SendApplyVolumeAsync());
        ConsumeFlag(ref requestStartSender, StartSender);
        ConsumeFlag(ref requestStopSender, StopSender);
    }

    private void ConsumeFlag(ref bool flag, Action action)
    {
        if (!flag) return;
        flag = false;
        action?.Invoke();
    }

    public async Task ConnectAsync()
    {
        try
        {
            EnsureClient();
            if (client.IsConnected || isConnecting)
                return;

            isConnecting = true;
            lastError = "";

            var builder = new MqttClientOptionsBuilder()
                .WithTcpServer(brokerHost, brokerPort)
                .WithClientId($"unity-playback-{Guid.NewGuid():N}");

            if (!string.IsNullOrWhiteSpace(mqttUser))
                builder = builder.WithCredentials(mqttUser, mqttPassword);

            var options = builder.Build();
            await client.ConnectAsync(options);

            var subscribeOptions = new MqttClientSubscribeOptionsBuilder()
                .WithTopicFilter(f => f.WithTopic(TopicState).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                .WithTopicFilter(f => f.WithTopic(TopicConfigReported).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                .WithTopicFilter(f => f.WithTopic(TopicEndpoint).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                .WithTopicFilter(f => f.WithTopic(TopicEvents).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                .WithTopicFilter(f => f.WithTopic(TopicCapabilities).WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce))
                .Build();

            await client.SubscribeAsync(subscribeOptions);
            isConnected = client.IsConnected;
            isConnecting = false;
        }
        catch (Exception ex)
        {
            isConnected = false;
            isConnecting = false;
            nextReconnectAt = Time.unscaledTime + reconnectIntervalSeconds;
            lastError = ex.Message;
            Debug.LogError($"[NexorAudioPlaybackMqttController] Error al conectar MQTT: {ex}");
        }
    }

    public async Task DisconnectAsync()
    {
        try
        {
            if (client != null && client.IsConnected)
                await client.DisconnectAsync();
        }
        catch (Exception ex)
        {
            lastError = ex.Message;
            Debug.LogWarning($"[NexorAudioPlaybackMqttController] Error al desconectar MQTT: {ex}");
        }

        isConnected = false;
        isConnecting = false;
    }

    public Task SendGetStateAsync() => PublishCommandAsync("get_state");
    public Task SendRestartAsync() => PublishCommandAsync("restart");
    public Task SendStandbyAsync() => PublishCommandAsync("standby");
    public Task SendResumeAsync() => PublishCommandAsync("resume");

    public async Task SendApplyVolumeAsync()
    {
        var payload = new ConfigDesiredEnvelope
        {
            config = new ConfigPayload
            {
                volume = desiredVolume
            }
        };

        string json = JsonUtility.ToJson(payload);
        await PublishJsonAsync(TopicConfigDesired, json);
        Debug.Log($"[NexorAudioPlaybackMqttController] config/desired -> {json}");
    }

    public void StartSender()
    {
        if (sender == null)
            return;

        sender.StartMicrophone();
        sender.StartSending();
    }

    public void StopSender()
    {
        if (sender == null)
            return;

        sender.StopSending();
        sender.StopMicrophone();
    }

    private async Task PublishCommandAsync(string action)
    {
        string json =
            "{\"msg_id\":\"" + Guid.NewGuid().ToString("N") + "\"," +
            "\"source\":\"unity-inspector\"," +
            "\"action\":\"" + action + "\"," +
            "\"params\":{}}";

        await PublishJsonAsync(TopicCmd, json);
        Debug.Log($"[NexorAudioPlaybackMqttController] cmd={action}");
    }

    private async Task PublishJsonAsync(string topic, string json)
    {
        if (client == null || !client.IsConnected)
        {
            lastError = "MQTT no conectado";
            Debug.LogWarning("[NexorAudioPlaybackMqttController] MQTT no conectado");
            return;
        }

        try
        {
            var message = new MqttApplicationMessageBuilder()
                .WithTopic(topic)
                .WithPayload(json)
                .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                .Build();

            await client.PublishAsync(message);
        }
        catch (Exception ex)
        {
            lastError = ex.Message;
            Debug.LogError($"[NexorAudioPlaybackMqttController] Error publicando en {topic}: {ex}");
        }
    }

    private async void OnDestroy()
    {
        await DisconnectAsync();
    }

    private async void OnApplicationQuit()
    {
        await DisconnectAsync();
    }
}
