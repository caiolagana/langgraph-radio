"""
broadcast.py — transmissão de áudio da rádio.

Ideia central ("tee do PCM"): uma única thread — o *pump* — mantém um relógio em
tempo real e é a ÚNICA que escreve áudio, bloco a bloco. Para cada bloco de PCM
ela faz o "tee": manda a MESMA amostra para dois destinos ao mesmo tempo:

    arquivo.mp3 --decode--> PCM (44.1kHz/16bit/estéreo)
                                  |
                            [ PUMP thread ]  (relógio em tempo real)
                             /            \\
                   placa de som       encoder MP3 (lame)
                (-> transmissor FM)         |
                                       fan-out em RAM
                                            |
                                  HTTP :8383  /stream.mp3  --> ngrok --> web

Para não deixar "respiro" entre as faixas, enqueue() não bloqueia até a faixa
tocar: ele bufferiza um pouco à frente (prefetch), o que permite ao grafo preparar
a próxima fala/música (LLM + TTS) enquanto a atual ainda toca. Se ainda assim o
buffer esvaziar, o pump emite silêncio como rede de segurança — o stream nunca
"morre" e o player não trava.
"""

import os
import queue
import threading
import time
from collections import deque
from pathlib import Path

import lameenc
import miniaudio

# --- formato único de áudio em toda a cadeia ---
SAMPLE_RATE = 44100
CHANNELS = 2
BYTES_PER_SAMPLE = 2                       # int16
BLOCK_FRAMES = 4096                        # ~93 ms por bloco
BLOCK_BYTES = BLOCK_FRAMES * CHANNELS * BYTES_PER_SAMPLE
SILENCE = bytes(BLOCK_BYTES)               # silêncio digital de 1 bloco
BLOCK_SECONDS = BLOCK_FRAMES / SAMPLE_RATE

# fan-out / buffer
CLIENT_QUEUE_MAX = 256                      # chunks MP3 em fila por ouvinte
BURST_BYTES = 64 * 1024                     # "burst-on-connect": ~2 s de MP3


# ----------------------------------------------------------------------------
# Saídas (sinks) para a placa de som — com fallback para modo "só web".
# ----------------------------------------------------------------------------
class _SoundCardSink:
    """Escreve PCM na placa via PortAudio (sounddevice). O write() bloqueia até
    haver espaço no buffer da placa — é ele que dá o ritmo em tempo real."""

    def __init__(self, device):
        import sounddevice as sd
        self._stream = sd.RawOutputStream(
            samplerate=SAMPLE_RATE,
            channels=CHANNELS,
            dtype="int16",
            blocksize=BLOCK_FRAMES,
            device=device,
        )
        self._stream.start()

    def write(self, block: bytes) -> None:
        self._stream.write(block)

    def close(self) -> None:
        try:
            self._stream.stop()
            self._stream.close()
        except Exception:  # noqa: BLE001
            pass


class _SleepSink:
    """Fallback sem placa (só web): mantém o ritmo dormindo o tempo de cada bloco."""

    def __init__(self):
        self._next = None

    def write(self, block: bytes) -> None:
        now = time.monotonic()
        if self._next is None:
            self._next = now
        self._next += BLOCK_SECONDS
        delay = self._next - now
        if delay > 0:
            time.sleep(delay)
        elif delay < -0.5:      # ficou muito para trás; ressincroniza
            self._next = now

    def close(self) -> None:
        pass


def _open_sink():
    """Abre a placa de som; se falhar (ou RADIO_OUTPUT=none), cai no modo só-web."""
    if os.getenv("RADIO_OUTPUT", "none").lower() == "none":
        print("[broadcast] RADIO_OUTPUT=none -> modo só-web (sem placa de som)")
        return _SleepSink()
    device = os.getenv("RADIO_AUDIO_DEVICE")  # índice ("0") ou trecho do nome
    if device is not None and device.isdigit():
        device = int(device)
    try:
        sink = _SoundCardSink(device)
        print(f"[broadcast] placa de som aberta (device={device!r})")
        return sink
    except Exception as e:  # noqa: BLE001
        print(f"[broadcast] falha ao abrir a placa ({e}); caindo p/ modo só-web")
        return _SleepSink()


# ----------------------------------------------------------------------------
# AudioHub: pump + encoder + fan-out
# ----------------------------------------------------------------------------
class AudioHub:
    def __init__(self, bitrate: int = 128, prefetch: int = 2):
        # Fila de faixas já decodificadas (pcm, titulo). O maxsize é o buffer de
        # prefetch: quando cheio, enqueue() bloqueia (backpressure) — assim o grafo
        # gera a próxima fala/música ENQUANTO a atual toca, sem respiro no ar.
        # ATENÇÃO: queue.Queue trata maxsize<=0 como fila INFINITA (sem backpressure,
        # RAM cresce sem limite). Por isso forçamos o mínimo de 1.
        prefetch = max(1, prefetch)
        self._sources: "queue.Queue" = queue.Queue(maxsize=prefetch)
        self._subs: set[queue.Queue] = set()
        self._subs_lock = threading.Lock()
        self._burst: deque[bytes] = deque()
        self._burst_len = 0
        self._now_playing: str | None = None
        self._stop = False
        self._sink = None
        self._bitrate = bitrate
        self._encoder = None

    # --- ciclo de vida ---
    def start(self) -> None:
        self._sink = _open_sink()
        self._encoder = lameenc.Encoder()
        self._encoder.set_bit_rate(self._bitrate)
        self._encoder.set_in_sample_rate(SAMPLE_RATE)
        self._encoder.set_channels(CHANNELS)
        self._encoder.set_quality(2)          # 2 = alta qualidade
        threading.Thread(target=self._pump, name="audio-pump", daemon=True).start()

    # --- API usada pelo grafo ---
    def enqueue(self, path: str, title: str | None = None) -> None:
        """Decodifica a faixa e a coloca na fila do pump. NÃO bloqueia até tocar:
        só aplica backpressure quando o buffer de prefetch está cheio. É isso que
        permite ao grafo preparar a próxima fala/música durante a reprodução atual
        (eliminando o respiro entre ciclos)."""
        try:
            pcm = self._decode(path)
        except Exception as e:  # noqa: BLE001 - rádio não pode cair por 1 arquivo
            print(f"[broadcast] falha ao decodificar {path}: {e}")
            return
        self._sources.put((pcm, title or Path(path).stem))   # bloqueia se cheio

    @property
    def now_playing(self) -> str | None:
        return self._now_playing

    # --- decodificação MP3 -> PCM normalizado ---
    @staticmethod
    def _decode(path: str) -> bytes:
        dec = miniaudio.decode_file(
            path,
            output_format=miniaudio.SampleFormat.SIGNED16,
            nchannels=CHANNELS,
            sample_rate=SAMPLE_RATE,
        )
        return dec.samples.tobytes()

    @staticmethod
    def _iter_blocks(pcm: bytes):
        for i in range(0, len(pcm), BLOCK_BYTES):
            yield pcm[i:i + BLOCK_BYTES]

    # --- o coração: relógio em tempo real + tee ---
    def _pump(self) -> None:
        cur = None  # iterador de blocos da faixa atual
        while not self._stop:
            block = None
            if cur is not None:
                block = next(cur, None)
                if block is None:           # faixa acabou
                    cur = None
            if cur is None:
                try:                        # próxima faixa já bufferizada?
                    pcm, title = self._sources.get_nowait()
                    self._now_playing = title
                    cur = self._iter_blocks(pcm)
                    continue
                except queue.Empty:         # nada pronto: silêncio (rede de segurança)
                    self._now_playing = None
                    block = SILENCE

            if len(block) < BLOCK_BYTES:     # completa o último bloco com silêncio
                block = block + bytes(BLOCK_BYTES - len(block))

            self._sink.write(block)          # destino 1: placa (dita o ritmo)
            self._feed(block)                # destino 2: encoder -> web

    # --- encoder + fan-out para os ouvintes ---
    def _feed(self, pcm_block: bytes) -> None:
        mp3 = self._encoder.encode(pcm_block)
        if mp3:
            self._broadcast(bytes(mp3))

    def _broadcast(self, data: bytes) -> None:
        # buffer de burst (para quem conecta no meio)
        self._burst.append(data)
        self._burst_len += len(data)
        while self._burst_len > BURST_BYTES and len(self._burst) > 1:
            self._burst_len -= len(self._burst.popleft())
        # copia para a fila de cada ouvinte (descarta se o ouvinte está lento)
        with self._subs_lock:
            for q in self._subs:
                try:
                    q.put_nowait(data)
                except queue.Full:
                    pass

    # --- gestão de ouvintes (usado pelo servidor HTTP) ---
    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=CLIENT_QUEUE_MAX)
        with self._subs_lock:
            self._subs.add(q)
        return q

    def unsubscribe(self, q: queue.Queue) -> None:
        with self._subs_lock:
            self._subs.discard(q)

    def burst_bytes(self) -> bytes:
        return b"".join(self._burst)


# ----------------------------------------------------------------------------
# Servidor HTTP (FastAPI + uvicorn), rodado numa thread separada.
# ----------------------------------------------------------------------------
_PLAYER_HTML = """<!doctype html>
<html lang="pt-br"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Rádio</title>
<style>
  body{font-family:system-ui,sans-serif;background:#111;color:#eee;
       display:flex;flex-direction:column;align-items:center;justify-content:center;
       min-height:100vh;margin:0;gap:1.2rem}
  h1{font-weight:600;margin:0}
  #now{opacity:.7;font-size:.95rem;min-height:1.2em}
  audio{width:min(90vw,420px)}
</style></head>
<body>
  <h1>📻 No ar</h1>
  <div id="now">carregando…</div>
  <audio controls autoplay src="/stream.mp3"></audio>
  <script>
    async function tick(){
      try{const r=await fetch('/nowplaying');const j=await r.json();
          document.getElementById('now').textContent=j.title?('🎵 '+j.title):'—';}
      catch(e){}
    }
    tick(); setInterval(tick, 3000);
  </script>
</body></html>"""


def build_server(hub: AudioHub, host: str = "0.0.0.0", port: int = 8383):
    """Cria o uvicorn.Server (não inicia)."""
    import uvicorn
    from fastapi import FastAPI
    from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse

    app = FastAPI()

    @app.get("/")
    def index():
        return HTMLResponse(_PLAYER_HTML)

    @app.get("/nowplaying")
    def nowplaying():
        return JSONResponse({"title": hub.now_playing})

    @app.get("/stream.mp3")
    def stream():
        q = hub.subscribe()

        def gen():
            try:
                yield hub.burst_bytes()          # burst-on-connect
                while True:
                    try:
                        yield q.get(timeout=10)
                    except queue.Empty:
                        continue                 # sem dados por 10s: segue esperando
            finally:
                hub.unsubscribe(q)

        headers = {"Cache-Control": "no-cache, no-store", "icy-name": "Radio"}
        return StreamingResponse(gen(), media_type="audio/mpeg", headers=headers)

    config = uvicorn.Config(app, host=host, port=port, log_level="warning")
    server = uvicorn.Server(config)
    server.install_signal_handlers = lambda: None  # necessário fora da main thread
    return server


def start_broadcast(host: str = "0.0.0.0", port: int = 8383,
                    bitrate: int = 128, prefetch: int = 2) -> AudioHub:
    """Liga o pump + servidor HTTP e devolve o hub para o grafo usar."""
    hub = AudioHub(bitrate=bitrate, prefetch=prefetch)
    hub.start()
    server = build_server(hub, host=host, port=port)
    threading.Thread(target=server.run, name="http-server", daemon=True).start()
    print(f"[broadcast] no ar em http://{host}:{port}  (rota do stream: /stream.mp3)")
    return hub
