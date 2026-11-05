#!/usr/bin/env python3
"""Enrichit une liste d'agences via Serper /places (nom + adresse -> fiche Google).

- q = "nom adresse" (sans parenthèses), 3 fiches max par agence.
- 1 thread par clé (SERPER_API_KEYS) : 7 clés = 7 agences traitées en parallèle.
- Fenêtre de 20 s max par agence ; si la réponse arrive avant, on enchaîne.
- Reprise automatique via progress.jsonl.
"""
import argparse, json, os, queue, sys, threading, time
from pathlib import Path

import pandas as pd
import requests

URL = "https://google.serper.dev/places"
WINDOW_S = 20        # fenêtre max par agence
MAX_RESULTS = 3      # fiches gardées par agence
MIN_GAP_S = 0.25     # pause mini entre 2 requêtes d'une même clé
BACKOFF_429_S = 3    # attente après un 429 (la fenêtre de 20 s continue de courir)

FIELDS = ["position", "title", "address", "latitude", "longitude", "rating",
          "ratingCount", "category", "phoneNumber", "website", "placeId", "cid"]


def build_query(nom, adresse):
    parts = [str(nom or "").strip(), str(adresse or "").strip()]
    q = " ".join(p for p in parts if p and p.lower() != "nan")
    return " ".join(q.replace("(", " ").replace(")", " ").split())


def search(key, q, deadline):
    """Retourne (status, places, key_ok). Tout reste dans la fenêtre `deadline`."""
    status = "timeout_20s"
    while time.time() < deadline:
        try:
            r = requests.post(URL, timeout=max(1, deadline - time.time()),
                              headers={"X-API-KEY": key, "Content-Type": "application/json"},
                              json={"q": q, "gl": "fr", "hl": "fr"})
        except requests.RequestException:
            status = "erreur_reseau"
            continue
        if r.status_code == 200:
            places = r.json().get("places") or []
            return ("ok" if places else "aucun_resultat"), places[:MAX_RESULTS], True
        if r.status_code == 429:
            status = "rate_limit"
            time.sleep(min(BACKOFF_429_S, max(0, deadline - time.time())))
        elif r.status_code in (400, 401, 403):      # clé invalide / crédits épuisés
            return "cle_invalide", [], False
        else:
            status = f"http_{r.status_code}"
            time.sleep(0.5)
    return status, [], True


def worker(wid, key, tasks, ctx):
    last = 0.0
    while time.time() < ctx["stop_at"]:
        try:
            i, nom, adresse = tasks.get_nowait()
        except queue.Empty:
            return
        q = build_query(nom, adresse)
        gap = MIN_GAP_S - (time.time() - last)
        if gap > 0:
            time.sleep(gap)
        t0 = last = time.time()
        status, places, key_ok = search(key, q, t0 + WINDOW_S)
        if not key_ok:
            print(f"  clé #{wid} retirée (invalide ou crédits épuisés)", flush=True)
            return                                   # la ligne n'est pas marquée : refaite au prochain run
        with ctx["lock"]:
            if status != "rate_limit":               # rate_limit = transitoire, on réessaiera
                ctx["f"].write(json.dumps({
                    "row": i + 2,
                    "nom": None if pd.isna(nom) else str(nom),
                    "adresse": None if pd.isna(adresse) else str(adresse),
                    "q": q, "status": status, "places": places}, ensure_ascii=False) + "\n")
                ctx["f"].flush()
                ctx["done"] += 1
            print(f"[{i+2}] k{wid} {status:14} {len(places)} fiche(s) {time.time()-t0:4.1f}s | {q[:60]}",
                  flush=True)


def export_xlsx(progress, out):
    rows = []
    for line in progress.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        d = json.loads(line)
        base = {"ligne_source": d["row"], "agence_source": d["nom"],
                "adresse_source": d["adresse"], "recherche_q": d["q"], "statut": d["status"]}
        if not d["places"]:
            rows.append(base)
        for p in d["places"]:
            rows.append({**base, **{f: p.get(f) for f in FIELDS}})
    df = pd.DataFrame(rows).sort_values(["ligne_source", "position"], na_position="first")
    with pd.ExcelWriter(out, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Fiches Google")
        ws = w.sheets["Fiches Google"]
        ws.freeze_panes = "A2"
        for col in ws.columns:
            ws.column_dimensions[col[0].column_letter].width = min(
                45, max(12, max(len(str(c.value or "")) for c in col[:200]) + 2))
    print(f"xlsx écrit : {out} ({len(df)} lignes)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/agences_prospection.xlsx")
    ap.add_argument("--sheet", default="Agences")
    ap.add_argument("--name-col", default="Nom de l'agence")
    ap.add_argument("--addr-col", default="Adresse complète")
    ap.add_argument("--progress", default="data/progress.jsonl")
    ap.add_argument("--output", default="output/resultats_places.xlsx")
    ap.add_argument("--limit", type=int, default=0, help="nb max d'agences dans ce run (0 = tout)")
    ap.add_argument("--max-minutes", type=float, default=330, help="arrêt propre avant les 6 h de GitHub")
    ap.add_argument("--test", action="store_true", help="5 agences, fichiers *_test séparés")
    a = ap.parse_args()

    if a.test:
        a.limit = 5
        a.progress, a.output = "data/progress_test.jsonl", "output/resultats_places_test.xlsx"

    keys = list(dict.fromkeys(k.strip() for k in
                              os.environ.get("SERPER_API_KEYS", "").replace("\n", ",").split(",") if k.strip()))
    if not keys:
        sys.exit("Aucune clé : renseigne SERPER_API_KEYS.")
    print(f"{len(keys)} clé(s) = {len(keys)} worker(s) en parallèle")

    df = pd.read_excel(a.input, sheet_name=a.sheet)
    progress = Path(a.progress); progress.parent.mkdir(parents=True, exist_ok=True)
    Path(a.output).parent.mkdir(parents=True, exist_ok=True)
    progress.touch()
    done_rows = {json.loads(l)["row"] for l in progress.read_text(encoding="utf-8").splitlines() if l.strip()}

    todo = [i for i in range(len(df)) if i + 2 not in done_rows]   # +2 = n° de ligne Excel
    total_todo = len(todo)
    if a.limit:
        todo = todo[:a.limit]

    tasks = queue.Queue()
    for i in todo:
        tasks.put((i, df.at[i, a.name_col], df.at[i, a.addr_col]))

    with progress.open("a", encoding="utf-8") as f:
        ctx = {"lock": threading.Lock(), "f": f, "done": 0, "stop_at": time.time() + a.max_minutes * 60}
        threads = [threading.Thread(target=worker, args=(n + 1, k, tasks, ctx), daemon=True)
                   for n, k in enumerate(keys)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    remaining = total_todo - ctx["done"]
    alive = ctx["done"] > 0 or remaining == 0
    print(f"Run terminé : {ctx['done']} traitées, {remaining} restantes")
    export_xlsx(progress, a.output)
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as o:
            o.write(f"remaining={remaining}\nalive={'true' if alive else 'false'}\n")


if __name__ == "__main__":
    main()
