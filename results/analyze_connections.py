import re
from collections import Counter
from pathlib import Path

RESULTS = Path(__file__).resolve().parent
SERVER = "100.22.128.174.8080"

HEADER = re.compile(
    r"^\d+\.\d+ IP (\S+) > (\S+): Flags \[([^\]]+)\], (.*)$"
)
SEQ = re.compile(r"\bseq (\d+)")
ACK = re.compile(r"\back (\d+)")

connections = []
active = {}
current = None


def process_header(match):
    global current

    source, destination, flags, details = match.groups()
    outgoing = destination == SERVER

    if not outgoing and source != SERVER:
        current = None
        return

    client = source if outgoing else destination
    seq_match = SEQ.search(details)
    ack_match = ACK.search(details)

    # A client SYN identifies a new connection incarnation.
    if outgoing and "S" in flags and "." not in flags:
        if seq_match is None:
            current = None
            return

        initial_seq = int(seq_match.group(1))
        existing = active.get(client)

        if existing is not None and existing["client_seq"] == initial_seq:
            existing["repeated_syns"] += 1
        else:
            existing = {
                "client": client,
                "client_seq": initial_seq,
                "server_seq": None,
                "synack": False,
                "handshake": False,
                "protocols": set(),
                "repeated_syns": 0,
                "fin": False,
                "rst": False,
            }
            connections.append(existing)
            active[client] = existing

        current = (existing, outgoing)
        return

    connection = active.get(client)
    current = (connection, outgoing) if connection else None

    if connection is None:
        return

    if not outgoing and "S" in flags and "." in flags:
        if seq_match and ack_match:
            expected = (connection["client_seq"] + 1) % (2**32)

            if int(ack_match.group(1)) == expected:
                connection["synack"] = True
                connection["server_seq"] = int(seq_match.group(1))

    elif outgoing and "." in flags and "S" not in flags:
        if connection["synack"] and ack_match:
            expected = (connection["server_seq"] + 1) % (2**32)

            if int(ack_match.group(1)) == expected:
                connection["handshake"] = True

    if "F" in flags:
        connection["fin"] = True
    if "R" in flags:
        connection["rst"] = True


with (RESULTS / "tcp-details.txt").open(
        encoding="utf-8", errors="replace"
) as file:
    for line in file:
        match = HEADER.match(line)

        if match:
            process_header(match)
            continue

        if current is None:
            continue

        connection, outgoing = current
        if not outgoing:
            continue

        if "GET /ski-servlet/leaderboard/" in line:
            connection["protocols"].add("WebSocket")

        if "POST /ski-servlet/skiers/" in line:
            connection["protocols"].add("HTTP")


def classify(connection):
    protocols = connection["protocols"]
    if len(protocols) == 1:
        return next(iter(protocols))
    return "Mixed" if protocols else "Unclassified"


counts = Counter(classify(c) for c in connections)

lines = [
    "TCP CONNECTION ANALYSIS",
    "",
    f"Distinct observed opening attempts: {len(connections)}",
    f"Repeated SYN packets: {sum(c['repeated_syns'] for c in connections)}",
    f"Attempts with matched SYN-ACK: {sum(c['synack'] for c in connections)}",
    f"Observed completed handshakes: {sum(c['handshake'] for c in connections)}",
    "",
]

for protocol in ("HTTP", "WebSocket", "Mixed", "Unclassified"):
    group = [c for c in connections if classify(c) == protocol]
    lines.append(
        f"{protocol}: openings={counts[protocol]}, "
        f"completed handshakes={sum(c['handshake'] for c in group)}, "
        f"connections with FIN={sum(c['fin'] for c in group)}, "
        f"connections with RST={sum(c['rst'] for c in group)}"
    )

lines.extend([
    "",
    "Classification uses captured client request paths.",
    "FIN and RST counts describe connections, not packet totals.",
    "A captured final client ACK is evidence of handshake completion "
    "at the capture point; it does not prove server receipt.",
    "New HTTP connections are not automatically reconnections: "
    "pool expansion and replacement are not distinguished here.",
])

summary = "\n".join(lines) + "\n"
print(summary)

(RESULTS / "tcp-classified-summary.txt").write_text(summary)