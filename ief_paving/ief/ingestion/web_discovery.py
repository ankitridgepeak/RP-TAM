from __future__ import annotations
import asyncio, re, json
from typing import List, Dict, Tuple
from urllib.parse import urljoin, urlparse
from urllib import robotparser

import httpx
from aiolimiter import AsyncLimiter
from bs4 import BeautifulSoup
import trafilatura
import phonenumbers

# Simple polite fetcher with robots.txt compliance
class PoliteFetcher:
    def __init__(self, rate_per_host: float = 1.0, timeout: float = 15.0):
        self.timeout = timeout
        self.limiters = {}
        self.robot_cache: Dict[str, robotparser.RobotFileParser] = {}
        self.headers = {"User-Agent": "IEF-Discovery/0.1 (+https://example.com/contact)"}

    def _limiter(self, host: str) -> AsyncLimiter:
        if host not in self.limiters:
            # rate_per_host req/sec => 1 per second minimum spacing
            self.limiters[host] = AsyncLimiter(1, 1)
        return self.limiters[host]

    async def allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        if base not in self.robot_cache:
            rp = robotparser.RobotFileParser()
            try:
                async with httpx.AsyncClient(timeout=self.timeout, headers=self.headers, follow_redirects=True) as client:
                    r = await client.get(urljoin(base, "/robots.txt"))
                    if r.status_code == 200:
                        rp.parse(r.text.splitlines())
                    else:
                        rp.parse([])
            except Exception:
                rp.parse([])
            self.robot_cache[base] = rp
        return self.robot_cache[base].can_fetch(self.headers["User-Agent"], url)

    async def get(self, url: str) -> str:
        parsed = urlparse(url)
        limiter = self._limiter(parsed.netloc)
        if not await self.allowed(url):
            return ""
        async with limiter:
            try:
                async with httpx.AsyncClient(timeout=self.timeout, headers=self.headers, follow_redirects=True) as client:
                    r = await client.get(url)
                    if r.status_code == 200 and r.headers.get("content-type","").startswith("text"):
                        return r.text
            except Exception:
                return ""
        return ""

SERVICE_PATHS = ["/", "/about", "/services", "/service", "/contact", "/areas-served", "/sitemap.xml"]

def extract_structured(html: str, base_url: str) -> Dict:
    out: Dict = {"name": "", "address": "", "city": "", "state": "", "postal_code": "", "phone": "", "website": base_url, "work_types": ""}
    if not html:
        return out
    soup = BeautifulSoup(html, "lxml")

    # Name/title
    title = (soup.title.string if soup.title else "") or ""
    out["name"] = title.strip()[:200]

    # JSON-LD LocalBusiness
    for tag in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        def _walk(node):
            nonlocal out
            if isinstance(node, dict):
                typ = node.get("@type") or node.get("@type".lower())
                if isinstance(typ, list):
                    types = [t.lower() for t in typ if isinstance(t, str)]
                elif isinstance(typ, str):
                    types = [typ.lower()]
                else:
                    types = []
                if any(t in types for t in ["localbusiness","contractor","homeandconstructionbusiness"]):
                    out["name"] = node.get("name") or out["name"]
                    addr = node.get("address") or {}
                    if isinstance(addr, dict):
                        out["address"] = addr.get("streetAddress") or out["address"]
                        out["city"] = addr.get("addressLocality") or out["city"]
                        out["state"] = addr.get("addressRegion") or out["state"]
                        out["postal_code"] = addr.get("postalCode") or out["postal_code"]
                    out["phone"] = node.get("telephone") or out["phone"]
            if isinstance(node, list):
                for it in node:
                    _walk(it)
            elif isinstance(node, dict):
                for v in node.values():
                    _walk(v)
        _walk(data)

    # Phone fallback
    if not out["phone"]:
        m = re.search(r"(\+?1[\s\-\.]?)?\(?\d{3}\)?[\s\-\.]?\d{3}[\s\-\.]?\d{4}", soup.get_text(" ", strip=True))
        if m:
            try:
                num = phonenumbers.parse(m.group(0), "US")
                if phonenumbers.is_valid_number(num):
                    out["phone"] = phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
            except Exception:
                pass

    # Work types keywords (quick summary)
    text = trafilatura.extract(html) or ""
    kws = []
    for k in ["asphalt", "paving", "sealcoat", "sealcoating", "chip seal", "driveway", "parking lot", "milling", "overlay"]:
        if re.search(rf"\b{k}\b", text, flags=re.I):
            kws.append(k)
    out["work_types"] = ", ".join(sorted(set(kws)))
    return out

async def crawl_domain(domain: str) -> Dict:
    fetcher = PoliteFetcher()
    base = f"https://{domain}"
    best = {}
    for p in SERVICE_PATHS:
        html = await fetcher.get(urljoin(base, p))
        if not html:
            continue
        data = extract_structured(html, base)
        # Prefer pages that yield phone + some keywords
        score = (1 if data.get("phone") else 0) + (1 if data.get("work_types") else 0)
        if not best or score > best.get("_score", 0):
            data["_score"] = score
            best = data
    if best:
        best["source_name"] = "web"
        return best
    return {}

async def crawl_domains(domains: List[str], limit: int = 1000) -> List[Dict]:
    out: List[Dict] = []
    sem = asyncio.Semaphore(20)
    async def _one(d):
        async with sem:
            try:
                data = await crawl_domain(d)
                if data:
                    out.append(data)
            except Exception:
                pass
    tasks = []
    for d in domains[:limit]:
        tasks.append(asyncio.create_task(_one(d)))
    await asyncio.gather(*tasks)
    return out