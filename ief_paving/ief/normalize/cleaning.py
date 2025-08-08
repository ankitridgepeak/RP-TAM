from __future__ import annotations
import re
import phonenumbers
import tldextract

def normalize_name(name: str) -> str:
    if not isinstance(name, str): return ''
    name = name.strip()
    name = re.sub(r'\s+', ' ', name)
    return name

def to_e164(phone: str) -> str:
    if not isinstance(phone, str) or not phone.strip(): return ''
    try:
        num = phonenumbers.parse(phone, 'US')
        if phonenumbers.is_valid_number(num):
            return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return ''
    return ''

def root_domain(url: str) -> str:
    if not isinstance(url, str) or not url.strip(): return ''
    ext = tldextract.extract(url if '://' in url else f'https://{url}')
    if not ext.registered_domain: return ''
    return ext.registered_domain.lower()