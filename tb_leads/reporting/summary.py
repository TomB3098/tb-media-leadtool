from __future__ import annotations

from collections import Counter


def summarize(scored: list[dict]) -> str:
    classes = Counter([x.get("score_class", "?") for x in scored])
    top = scored[:5]

    lines = []
    lines.append(f"Leads gesamt (gefiltert): {len(scored)}")
    lines.append(f"Klassenverteilung: A={classes.get('A',0)} B={classes.get('B',0)} C={classes.get('C',0)}")
    if top:
        lines.append("Top 5:")
        for i, lead in enumerate(top, start=1):
            lines.append(f"  {i}. {lead.get('name')} ({lead.get('city')}) - {lead.get('score_total')} [{lead.get('score_class')}]")
    return "\n".join(lines)
