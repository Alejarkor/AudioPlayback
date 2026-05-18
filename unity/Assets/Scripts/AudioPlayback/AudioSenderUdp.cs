using UnityEngine;
using System;
using System.Net.Sockets;
using System.Net;

public class AudioSenderUdp : MonoBehaviour
{
    [Header("Captura")]
    public bool autoStartMicrophone = false;
    public string microphoneDeviceName = "";
    public int sampleRate = 48000;
    public int channels = 1;
    public bool is16Bits = true;

    [Header("Envío UDP")]
    public string remoteIp = "127.0.0.1";
    public int remotePort = 1236;
    public bool sendEnabled = false;
    public bool runInBackground = true;
    public bool logPackets = false;

    [Header("Ganancia")]
    public float inputGain = 1.0f;

    [Header("Comandos one-shot desde Inspector")]
    public bool requestStartMicrophone;
    public bool requestStopMicrophone;
    public bool requestStartSending;
    public bool requestStopSending;
    public bool requestRestartAudio;

    [Header("Estado runtime")]
    public bool microphoneActive;
    public int packetsSent;
    public int samplesSent;
    public string lastError;

    private UdpClient udpClient;
    private AudioClip microphoneClip;
    private int microphoneSamplePosition;
    private string activeMicrophoneDevice;

    void Start()
    {
        Application.runInBackground = runInBackground;
        udpClient = new UdpClient();

        if (autoStartMicrophone)
            StartMicrophone();
    }

    void Update()
    {
        ProcessInspectorRequests();
        PumpMicrophoneAudio();
    }

    private void ProcessInspectorRequests()
    {
        ConsumeFlag(ref requestStartMicrophone, StartMicrophone);
        ConsumeFlag(ref requestStopMicrophone, StopMicrophone);
        ConsumeFlag(ref requestStartSending, StartSending);
        ConsumeFlag(ref requestStopSending, StopSending);
        ConsumeFlag(ref requestRestartAudio, RestartAudio);
    }

    private void ConsumeFlag(ref bool flag, Action action)
    {
        if (!flag) return;
        flag = false;
        action?.Invoke();
    }

    public void StartMicrophone()
    {
        if (microphoneActive)
            return;

        try
        {
            if (Microphone.devices == null || Microphone.devices.Length == 0)
            {
                lastError = "No hay micrófonos disponibles";
                Debug.LogWarning("[AudioSenderUdp] No hay micrófonos disponibles");
                return;
            }

            activeMicrophoneDevice = string.IsNullOrWhiteSpace(microphoneDeviceName)
                ? Microphone.devices[0]
                : microphoneDeviceName;

            microphoneClip = Microphone.Start(activeMicrophoneDevice, true, 1, sampleRate);
            microphoneSamplePosition = 0;
            microphoneActive = true;
            Debug.Log($"[AudioSenderUdp] Micrófono arrancado: {activeMicrophoneDevice}");
        }
        catch (Exception ex)
        {
            lastError = ex.Message;
            Debug.LogError($"[AudioSenderUdp] Error arrancando micrófono: {ex}");
        }
    }

    public void StopMicrophone()
    {
        if (!microphoneActive)
            return;

        try
        {
            if (!string.IsNullOrWhiteSpace(activeMicrophoneDevice))
                Microphone.End(activeMicrophoneDevice);
        }
        catch (Exception ex)
        {
            lastError = ex.Message;
            Debug.LogWarning($"[AudioSenderUdp] Error parando micrófono: {ex}");
        }

        microphoneClip = null;
        microphoneSamplePosition = 0;
        microphoneActive = false;
        Debug.Log("[AudioSenderUdp] Micrófono detenido");
    }

    public void StartSending()
    {
        sendEnabled = true;
        Debug.Log("[AudioSenderUdp] Envío UDP activado");
    }

    public void StopSending()
    {
        sendEnabled = false;
        Debug.Log("[AudioSenderUdp] Envío UDP desactivado");
    }

    public void RestartAudio()
    {
        StopMicrophone();
        StartMicrophone();
    }

    public void SetRemoteEndpoint(string ip, int port)
    {
        remoteIp = ip;
        remotePort = port;
        Debug.Log($"[AudioSenderUdp] Endpoint remoto = {remoteIp}:{remotePort}");
    }

    public void SetSendEnabled(bool enabled)
    {
        sendEnabled = enabled;
    }

    private void PumpMicrophoneAudio()
    {
        if (!microphoneActive || microphoneClip == null)
            return;

        int currentPos = Microphone.GetPosition(activeMicrophoneDevice);
        if (currentPos < 0)
            return;

        int totalSamples = microphoneClip.samples * microphoneClip.channels;
        int readPos = microphoneSamplePosition;
        int writePos = currentPos * microphoneClip.channels;

        if (writePos == readPos)
            return;

        int samplesToRead = writePos > readPos
            ? writePos - readPos
            : (totalSamples - readPos) + writePos;

        if (samplesToRead <= 0)
            return;

        float[] buffer = new float[samplesToRead];

        if (writePos > readPos)
        {
            microphoneClip.GetData(buffer, readPos / microphoneClip.channels);
        }
        else
        {
            int firstPart = totalSamples - readPos;
            float[] tmpA = new float[firstPart];
            float[] tmpB = new float[writePos];
            microphoneClip.GetData(tmpA, readPos / microphoneClip.channels);
            if (writePos > 0)
                microphoneClip.GetData(tmpB, 0);
            Array.Copy(tmpA, 0, buffer, 0, tmpA.Length);
            if (tmpB.Length > 0)
                Array.Copy(tmpB, 0, buffer, tmpA.Length, tmpB.Length);
        }

        microphoneSamplePosition = writePos;

        if (!sendEnabled)
            return;

        SendSamples(buffer);
    }

    private void SendSamples(float[] samples)
    {
        try
        {
            byte[] payload = is16Bits
                ? ConvertFloatsTo16BitPcm(samples)
                : ConvertFloatsTo24BitPcm(samples);

            udpClient.Send(payload, payload.Length, remoteIp, remotePort);
            packetsSent++;
            samplesSent += samples.Length;

            if (logPackets)
                Debug.Log($"[AudioSenderUdp] Packet enviado: {payload.Length} bytes a {remoteIp}:{remotePort}");
        }
        catch (Exception ex)
        {
            lastError = ex.Message;
            Debug.LogError($"[AudioSenderUdp] Error enviando UDP: {ex}");
        }
    }

    private byte[] ConvertFloatsTo16BitPcm(float[] samples)
    {
        byte[] bytes = new byte[samples.Length * 2];
        for (int i = 0; i < samples.Length; i++)
        {
            float s = Mathf.Clamp(samples[i] * inputGain, -1f, 1f);
            short pcm = (short)Mathf.RoundToInt(s * 32767f);
            int idx = i * 2;
            bytes[idx] = (byte)(pcm & 0xFF);
            bytes[idx + 1] = (byte)((pcm >> 8) & 0xFF);
        }
        return bytes;
    }

    private byte[] ConvertFloatsTo24BitPcm(float[] samples)
    {
        byte[] bytes = new byte[samples.Length * 3];
        for (int i = 0; i < samples.Length; i++)
        {
            float s = Mathf.Clamp(samples[i] * inputGain, -1f, 1f);
            int pcm = Mathf.RoundToInt(s * 8388607f);
            int idx = i * 3;
            bytes[idx] = (byte)(pcm & 0xFF);
            bytes[idx + 1] = (byte)((pcm >> 8) & 0xFF);
            bytes[idx + 2] = (byte)((pcm >> 16) & 0xFF);
        }
        return bytes;
    }

    private void OnDestroy()
    {
        StopMicrophone();
        try { udpClient?.Close(); } catch { }
        udpClient = null;
    }

    private void OnApplicationQuit()
    {
        StopMicrophone();
        try { udpClient?.Close(); } catch { }
        udpClient = null;
    }
}
