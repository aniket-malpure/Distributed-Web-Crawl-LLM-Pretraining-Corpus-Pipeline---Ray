"""Crawl-strategy experiments on a simulated web graph (fully offline, seeded).

Question answered: given a fixed fetch budget, which frontier policy yields the most *useful*
(post-filter) documents? Policies:

* ``bfs``    - FIFO frontier (baseline)
* ``random`` - uniform random frontier pick
* ``ucb``    - pick the frontier URL whose *domain* has the best upper-confidence-bound on
               observed yield (a multi-armed bandit over domains)

This mirrors the real decision "where do we spend crawl budget?" and gives a measurable
harvest rate, which is the number that goes in the experiment log.
"""
from __future__ import annotations

import math
import random
from collections import defaultdict, deque
from dataclasses import dataclass


@dataclass
class SimWeb:
    n_domains: int = 120
    pages_per_domain: int = 60
    out_degree: int = 6
    cross_domain_prob: float = 0.35
    seed: int = 11

    def __post_init__(self) -> None:
        rng = random.Random(self.seed)
        # Latent per-domain quality: most domains are poor, a minority are rich in usable text.
        self.quality = [min(0.98, rng.betavariate(0.6, 1.6)) for _ in range(self.n_domains)]
        self.useful: dict[tuple[int, int], bool] = {}
        self.links: dict[tuple[int, int], list[tuple[int, int]]] = {}
        for d in range(self.n_domains):
            for p in range(self.pages_per_domain):
                self.useful[(d, p)] = rng.random() < self.quality[d]
        for d in range(self.n_domains):
            for p in range(self.pages_per_domain):
                outs = []
                for _ in range(self.out_degree):
                    if rng.random() < self.cross_domain_prob:
                        # Cross links favour popular (high-quality AND random) domains weakly.
                        dd = rng.randrange(self.n_domains)
                        outs.append((dd, rng.randrange(self.pages_per_domain)))
                    else:
                        outs.append((d, rng.randrange(self.pages_per_domain)))
                self.links[(d, p)] = outs
        self.seeds = [(rng.randrange(self.n_domains), 0) for _ in range(10)]


def run_strategy(web: SimWeb, strategy: str, budget: int, seed: int = 0) -> dict:
    rng = random.Random(seed)
    frontier: deque | list = deque() if strategy == "bfs" else []
    seen: set[tuple[int, int]] = set()
    for s in web.seeds:
        if s not in seen:
            seen.add(s)
            frontier.append(s)
    dom_fetch: dict[int, int] = defaultdict(int)
    dom_useful: dict[int, int] = defaultdict(int)
    total_fetch = total_useful = 0
    curve: list[int] = []

    def ucb(item: tuple[int, int]) -> float:
        d = item[0]
        n = dom_fetch[d]
        if n == 0:
            return float("inf")  # try every domain at least once
        return dom_useful[d] / n + math.sqrt(2.0 * math.log(total_fetch + 1) / n)

    while frontier and total_fetch < budget:
        if strategy == "bfs":
            item = frontier.popleft()  # type: ignore[union-attr]
        elif strategy == "random":
            i = rng.randrange(len(frontier))
            frontier[i], frontier[-1] = frontier[-1], frontier[i]
            item = frontier.pop()  # type: ignore[union-attr]
        elif strategy == "ucb":
            # Sample a bounded candidate set so selection stays O(k) not O(frontier).
            k = min(64, len(frontier))
            idx = rng.sample(range(len(frontier)), k)
            best = max(idx, key=lambda i: (ucb(frontier[i]), rng.random()))
            frontier[best], frontier[-1] = frontier[-1], frontier[best]
            item = frontier.pop()  # type: ignore[union-attr]
        else:
            raise ValueError(f"unknown strategy {strategy!r}")

        total_fetch += 1
        dom_fetch[item[0]] += 1
        if web.useful[item]:
            total_useful += 1
            dom_useful[item[0]] += 1
        curve.append(total_useful)
        for nxt in web.links[item]:
            if nxt not in seen:
                seen.add(nxt)
                frontier.append(nxt)

    return {
        "strategy": strategy,
        "fetched": total_fetch,
        "useful": total_useful,
        "harvest_rate": round(total_useful / max(1, total_fetch), 4),
        "domains_touched": len(dom_fetch),
        "curve": curve,
    }


def compare(
    budget: int = 3000,
    seeds: int = 5,
    strategies: tuple[str, ...] = ("bfs", "random", "ucb"),
    **web_kwargs,
) -> dict:
    """Average harvest rate across several simulated webs (different graph seeds)."""
    agg: dict[str, list[float]] = defaultdict(list)
    domains: dict[str, list[int]] = defaultdict(list)
    for s in range(seeds):
        web = SimWeb(seed=100 + s, **web_kwargs)
        for strat in strategies:
            r = run_strategy(web, strat, budget, seed=s)
            agg[strat].append(r["harvest_rate"])
            domains[strat].append(r["domains_touched"])
    summary = {
        strat: {
            "mean_harvest_rate": round(sum(v) / len(v), 4),
            "min": min(v),
            "max": max(v),
            "mean_domains_touched": round(sum(domains[strat]) / len(domains[strat]), 1),
        }
        for strat, v in agg.items()
    }
    base = summary[strategies[0]]["mean_harvest_rate"]
    for strat in summary:
        summary[strat]["lift_vs_" + strategies[0]] = round(
            summary[strat]["mean_harvest_rate"] / base - 1.0, 4
        ) if base else 0.0
    return {"budget": budget, "webs": seeds, "results": summary}
