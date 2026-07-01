# langgraph-radio

Rádio autônoma construída com [LangGraph](https://github.com/langchain-ai/langgraph): um locutor de IA introduz músicas, lê intervenções/notícias e toca a playlist em loop contínuo. O áudio sai **ao mesmo tempo na placa de som** (para um transmissor de rádio FM físico) **e num stream HTTP** que pode ser publicado na web via Cloudflare Tunnel.

## Como funciona

O grafo (`radio.py`) executa um ciclo por música:

1. **ingest** — monta/recarrega a playlist a partir de `mp3/`, consome arquivos `.txt` de `news/` como intervenções e prepara a próxima fala.
2. **router1** — se houver intervenção pendente, fala direto; senão escolhe entre gerar uma fala com IA (`brain`) ou usar uma fala padrão (`locutor`).
3. **brain** — o LLM (Claude) gera a fala do locutor, com acesso à ferramenta `hora_atual`.
4. **speak** — sintetiza a fala via `edge-tts` e toca.
5. **play_song** — toca a música atual até o fim.

O estado é persistido em um checkpoint SQLite (`SqliteSaver`).

![grafo](grafo.png)

## Arquitetura de áudio (tee do PCM)

O playback vive em `broadcast.py`. A ideia central é o **tee do PCM**: uma única
thread — o *pump* — mantém um relógio em tempo real e é a única que escreve áudio,
bloco a bloco (~93 ms). Para cada bloco, ela manda a **mesma amostra PCM** para
dois destinos simultâneos: a placa de som e o encoder do stream web.

```
  arquivo.mp3 ──decode──► PCM 44.1kHz/16bit/estéreo   (miniaudio normaliza tudo)
   (música ou                     │
    fala TTS)                     ▼
                        ┌───────────────────┐
                        │  PUMP (1 thread)  │  relógio em tempo real, bloco a bloco
                        └───────────────────┘
                          │                 │  (o MESMO bloco)
              ┌───────────┘                 └───────────┐
              ▼                                         ▼
      placa de som (sounddevice)                encoder MP3 (lameenc)
      → transmissor FM físico                          │  fluxo MP3 contínuo
                                                        ▼
                                          fan-out em RAM (1 fila por ouvinte
                                          + buffer de "burst-on-connect")
                                                        │
                                       servidor HTTP :8383 (FastAPI/uvicorn)
                                          GET /stream.mp3  ──cloudflared──► web
```

Pontos do desenho:

- **Um só stream contínuo.** As músicas e as falas são decodificadas para um PCM
  único (44.1 kHz, 16 bits, estéreo) e concatenadas pelo pump num único fluxo MP3
  — é o modelo de rádio de internet (estilo Icecast), não "um arquivo por vez".
- **Ritmo em tempo real.** `sounddevice` escreve na placa em modo bloqueante: é o
  próprio hardware que dá o compasso. Sem placa (`RADIO_OUTPUT=none`), um sink de
  fallback mantém o ritmo por tempo.
- **Sem respiro entre faixas (prefetch).** `enqueue()` não bloqueia até a faixa
  tocar — ele decodifica e coloca a faixa numa fila com buffer (`RADIO_PREFETCH`,
  padrão 2). Quando o buffer enche, o `enqueue` bloqueia (backpressure), pausando o
  grafo no compasso da reprodução. Assim o LLM e o TTS do **próximo** ciclo são
  preparados *enquanto a música atual toca*, e o pump emenda uma faixa na outra sem
  silêncio. Se, mesmo assim, a geração não acompanhar (buffer vazio), o pump emite
  silêncio digital como rede de segurança — o stream nunca "morre".
- **Burst-on-connect.** Quem sintoniza no meio recebe primeiro um pedaço recente do
  buffer (~64 KB), para o navegador engatar rápido.
- **Metadados.** `/nowplaying` devolve o título da faixa atual; a página `/` traz um
  player HTML que mostra o "tocando agora".

### Threads

| Thread | Papel |
| --- | --- |
| principal (grafo) | loop LangGraph; `speak`/`play_song` entregam o PCM ao pump e bloqueiam até a faixa terminar |
| `audio-pump` | o relógio: escreve na placa + alimenta o encoder, em tempo real |
| `http-server` | uvicorn servindo `/stream.mp3`, `/` (player) e `/nowplaying` |

### Publicar na web (Cloudflare Tunnel)

O stream precisa de uma URL pública HTTPS. Use o **Cloudflare Tunnel** (`cloudflared`):

```bash
# instalar uma vez (sem sudo)
curl -L https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64 \
  -o ~/.local/bin/cloudflared && chmod +x ~/.local/bin/cloudflared

# subir o túnel (a cada vez que a rádio for ao ar)
cloudflared tunnel --url http://localhost:8383
```

Ele imprime uma linha `https://xxxx.trycloudflare.com` — essa é a URL pública. O
player fica em `https://xxxx.trycloudflare.com/` e o stream em `…/stream.mp3`.

**Por que não o ngrok grátis?** O plano grátis do ngrok intercepta requisições de
navegador e devolve uma página de aviso em HTML (`ERR_NGROK_6024`) no lugar do áudio.
Como a tag `<audio>` não consegue enviar o header `ngrok-skip-browser-warning` que
pula esse aviso, a reprodução falha (o browser bloqueia com `OpaqueResponseBlocking`).
O Cloudflare Tunnel não injeta interstício, então o `<audio>` toca direto.

**Notas:**

- No modo *quick tunnel* (sem conta) a URL muda a cada execução. Para uma URL fixa,
  crie um *named tunnel* com uma conta Cloudflare grátis + um domínio próprio
  (`cloudflared tunnel login`, `create`, rota DNS).
- Essa URL do túnel é o que você entrega aos ouvintes. No site (Angular) ela é
  publicada num documento do Firestore (`config/radio` → campo `url`) e lida ao vivo,
  então basta atualizar esse campo a cada nova URL — sem redeploy do site.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # e preencha ANTHROPIC_API_KEY
```

Coloque arquivos `.mp3` em `mp3/`. Opcionalmente, adicione arquivos `.txt` em `news/` (cada arquivo vira uma intervenção falada).

## Uso

```bash
python radio.py
```

`Ctrl+C` para tirar a rádio do ar. A rádio já sobe no ar em `http://localhost:8383`.

### Variáveis de ambiente

| Variável | Padrão | Efeito |
| --- | --- | --- |
| `RADIO_PORT` | `8383` | Porta do servidor HTTP do stream. |
| `RADIO_PREFETCH` | `2` | Faixas bufferizadas à frente (elimina o respiro entre ciclos). Mínimo forçado de `1` — valores `<= 0` não desligam o buffer, apenas dariam uma fila infinita. |
| `RADIO_AUDIO_DEVICE` | (padrão do SO) | Dispositivo de saída da placa. Índice (`"0"`) ou trecho do nome. Use para mandar o áudio à saída ligada no transmissor FM. |
| `RADIO_OUTPUT` | — | `none` desliga a placa e transmite **só na web**. |

Para listar os dispositivos disponíveis:

```bash
python -c "import sounddevice as sd; print(sd.query_devices())"
```
