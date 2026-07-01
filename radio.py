import asyncio
import edge_tts
import operator
import random
import os
from dotenv import load_dotenv
from pathlib import Path
from datetime import datetime
from typing import Annotated, TypedDict, Optional, Literal
from langgraph.graph import END, START, StateGraph, MessagesState
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.graph.message import add_messages
from langgraph.checkpoint.sqlite import SqliteSaver
from langgraph.prebuilt import tools_condition, ToolNode
from langchain_core.tools import tool
from langchain_core.messages import SystemMessage, HumanMessage, RemoveMessage
from langchain_anthropic import ChatAnthropic
from broadcast import start_broadcast


BASE = Path(__file__).parent
load_dotenv(BASE / ".env")
MP3_DIR = BASE / "mp3"
NEWS_DIR = BASE / "news"
VOICE = "pt-BR-AntonioNeural"        # voz do locutor (ou "pt-BR-FranciscaNeural")
SPEECH_RATE = "+30%"                 # velocidade da fala (ex.: "+50%", "-10%")
AUDIO_DIR = BASE / ".audio"          # falas TTS geradas (efêmeras)
DB_PATH = BASE / "radio2.sqlite"      # checkpoint persistente
LLM_MODEL = "claude-haiku-4-5-20251001"
PERSONA = (
    "Você é um locutor de rádio brasileiro carismático, espontâneo e caloroso. "
    "Fale em português do Brasil, em tom coloquial de rádio. "
    "Produza APENAS a fala a ser lida em voz alta — sem aspas, sem marcações de "
    "cena, sem emojis, sem listar opções. Seja conciso (1 a 2 frases)."
)

# Hub de transmissão (pump + encoder + servidor HTTP). Inicializado em main().
HUB = None

class RadioState(TypedDict):
    messages: Annotated[list, add_messages] # Inherited in MessagesState
    playlist: list[str]
    now_playing: str
    intervencoes: list[str]
    to_speak: Optional[str]


CH1 = 'messages'
CH2 = 'playlist'
CH3 = 'intervencoes'
CH4 = 'to_speak'
CH5 = 'now_playing'


def ingest(state: RadioState) -> dict:
    update: dict = {}

    # Monta playlist com arquivos mp3, se a playlist estiver vazia
    playlist = list(state.get("playlist") or [])
    if not playlist:
        songs = [str(p) for p in MP3_DIR.glob("*.mp3")]
        random.shuffle(songs)
        playlist = songs
        if songs:
            print(f"[ingest] playlist refilada com {len(songs)} músicas")
    now_playing = playlist.pop(0)
    update[CH5] = now_playing
    print(f"[ingest] A próxima música será {now_playing}")
    update[CH2] = playlist

    # Consome arquivos .txt de news/ (cada arquivo = uma intervenção) e os
    # move para news/lidas.
    novas_intervencoes: list[str] = []
    lidas_dir = NEWS_DIR / "lidas"
    lidas_dir.mkdir(parents=True, exist_ok=True)
    for f in sorted(NEWS_DIR.glob("*.txt")):
        try:
            text = f.read_text(encoding="utf-8").strip()
            if text:
                novas_intervencoes.append(text)
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            f.rename(lidas_dir / f"{ts}_{f.name}")
        except Exception as e:  # noqa: BLE001
            print(f"[ingest] erro lendo {f.name}: {e}")
    fila_intervencoes = list(state.get(CH3) or []) + novas_intervencoes
    # Se houver intervenções a fazer, coloca a mais antiga no canal to_speak
    # Este canal será priorizado pelo router1
    if len(fila_intervencoes) > 0:
        update[CH4] = fila_intervencoes.pop(0)
    else:
        update[CH4] = None
    update[CH3] = fila_intervencoes

    # Reseta a conversa do ciclo anterior e inicia nova mensagem
    instruction = f"Introduza a próxima música a ser tocada: {Path(now_playing).stem}"
    update[CH1] = [
        RemoveMessage(id=REMOVE_ALL_MESSAGES),
        SystemMessage(content=PERSONA),
        HumanMessage(content=instruction)
    ]

    return update


def router1(state: RadioState) -> Literal['speak', 'brain', 'locutor']:
    if state[CH4]:
        return 'speak'
    return 'brain' if random.random() < 0.35 else 'locutor'



llm = ChatAnthropic(model=LLM_MODEL, temperature=0.9)

@tool
def hora_atual() -> str:
    """Retorna a hora atual no formato HH:MM, para o locutor situar o ouvinte."""
    from datetime import datetime, timedelta
    return (datetime.now() + timedelta(minutes=3)).strftime("%H:%M")


TOOLS = [hora_atual]
llm = llm.bind_tools(TOOLS)
tool_node = ToolNode(TOOLS)


def brain(state: RadioState) -> dict:
    print("[brain] montando mensagem IA")
    return {CH1: [llm.invoke(state[CH1])]}


def finalize_speech(state: RadioState) -> dict:
    """Monta o texto final a ser falado a partir da resposta do LLM."""
    ai_text = ""
    for m in reversed(state[CH1]):
        if m.type == "ai" and isinstance(m.content, str) and m.content.strip():
            ai_text = m.content.strip()
            break
    return {CH4: ai_text}


def locutor(state: RadioState) -> dict:
    texts = [
        "Fique com a próxima música",
        "Cuide sempre da natureza e ouça a próxima música",
        "Recicle sempre seu lixo e vamos para a próxima música",
        "Plante uma árvore e ouça a próxima música"
        ]
    return {CH4: random.choice(texts)}


def _say(text):
    out = AUDIO_DIR / "speech.mp3"
    try:
        asyncio.run(edge_tts.Communicate(text, VOICE, rate=SPEECH_RATE).save(str(out)))
    except Exception as e:  # noqa: BLE001 - rádio não pode cair por falha de TTS
        print(f"[speak] falha no TTS, pulando fala: {e}")
        return
    HUB.enqueue(str(out), title="🎙️ locutor")


def speak(state: RadioState) -> dict:
    """Sintetiza e toca a fala do locutor."""
    text = state.get(CH4) or ""
    if text:
        print(f"[speak] {text}")
        _say(text)
    return {}


def play_song(state: RadioState) -> dict:
    """Toca a música atual até o fim."""
    path = state.get(CH5)
    if not path or not Path(path).exists():
        print(f"[play] música inválida, pulando: {path}")
        return {}
    print(f"[play] tocando: {state.get(CH5)}")
    try:
        HUB.enqueue(path, title=Path(path).stem)
    except Exception as e:  # noqa: BLE001
        print(f"[play] erro ao tocar {path}, pulando: {e}")
    return {}


def build_app(checkpointer):
    grafo = StateGraph(RadioState)
    grafo.add_node('ingest', ingest)
    grafo.add_node('brain', brain)
    grafo.add_node('locutor', locutor)
    grafo.add_node('tools', tool_node)
    grafo.add_node('speak', speak)
    grafo.add_node("finalize_speech", finalize_speech)
    grafo.add_node("play_song", play_song)

    grafo.add_edge(START, 'ingest')
    grafo.add_conditional_edges(
        source='ingest',
        path=router1
    )
    grafo.add_conditional_edges(
        source="brain",
        path=tools_condition,
        path_map={"tools": "tools", END: "finalize_speech"}
    )
    grafo.add_edge("tools", "brain")
    grafo.add_edge('finalize_speech', 'speak')
    grafo.add_edge('locutor', 'speak')
    grafo.add_edge('speak', 'play_song')
    grafo.add_edge('play_song', END)

    return grafo.compile(checkpointer=checkpointer)

def main():
    if not os.getenv("ANTHROPIC_API_KEY"):
        raise SystemExit("Defina ANTHROPIC_API_KEY no arquivo .env")

    global HUB
    HUB = start_broadcast(
        port=int(os.getenv("RADIO_PORT", "8383")),
        prefetch=int(os.getenv("RADIO_PREFETCH", "1")),
    )

    config = {"configurable": {"thread_id": "radio-2"}}
    with SqliteSaver.from_conn_string(str(DB_PATH)) as checkpointer:
        app = build_app(checkpointer)

        # Desenha o grafo (opcional; ignora se não houver rede pra mermaid.ink).
        try:
            (BASE / "grafo.png").write_bytes(app.get_graph().draw_mermaid_png())
        except Exception:  # noqa: BLE001
            pass

        print("=== Rádio no ar (Ctrl+C para parar) ===")
        try:
            while True:
                app.invoke({}, config=config)
        except KeyboardInterrupt:
            print("\n=== Rádio fora do ar ===")


if __name__ == "__main__":
    main()