"""
Substack RSS feed parser.

Fetches and caches posts from the Substack RSS feed.
Substack blocks iframe embedding via CSP frame-ancestors,
so we parse the RSS server-side and serve it as JSON.
"""

import re
import logging
import threading
import requests
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

FEED_URL = 'https://theparrable.substack.com/feed'
CACHE_TTL_HOURS = 1

_cache = {
    'posts': [],
    'fetched_at': None,
}
_lock = threading.Lock()


def _strip_html(html):
    """Remove HTML tags, keeping only text."""
    if not html:
        return ''
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'\s+', ' ', text).strip()
    return text


def _parse_rss(xml_text):
    """Parse RSS XML into a list of post dicts (no external dependency)."""
    posts = []
    items = re.findall(r'<item>(.*?)</item>', xml_text, re.DOTALL)

    for item in items:
        title_m = re.search(r'<title><!\[CDATA\[(.*?)\]\]></title>', item, re.DOTALL)
        desc_m = re.search(r'<description><!\[CDATA\[(.*?)\]\]></description>', item, re.DOTALL)
        link_m = re.search(r'<link>(.*?)</link>', item)
        date_m = re.search(r'<pubDate>(.*?)</pubDate>', item)
        img_m = re.search(r'<enclosure url="(.*?)"', item)

        title = title_m.group(1) if title_m else ''
        description = desc_m.group(1) if desc_m else ''
        link = link_m.group(1) if link_m else ''
        pub_date_str = date_m.group(1) if date_m else ''
        image = img_m.group(1) if img_m else ''

        # Parse date
        pub_date = None
        if pub_date_str:
            try:
                pub_date = datetime.strptime(pub_date_str, '%a, %d %b %Y %H:%M:%S %Z')
            except ValueError:
                try:
                    pub_date = datetime.strptime(pub_date_str[:25], '%a, %d %b %Y %H:%M:%S')
                except ValueError:
                    pass

        # Clean description to plain text, truncate for preview
        preview = _strip_html(description)
        if len(preview) > 280:
            preview = preview[:277] + '...'

        posts.append({
            'title': title,
            'description': preview,
            'url': link,
            'published_at': pub_date.strftime('%b %d, %Y') if pub_date else '',
            'published_iso': pub_date.isoformat() if pub_date else '',
            'image': image,
        })

    return posts


def get_substack_posts(force_refresh=False):
    """Return cached Substack posts, refreshing if stale."""
    with _lock:
        now = datetime.utcnow()
        if (not force_refresh
                and _cache['fetched_at']
                and (now - _cache['fetched_at']) < timedelta(hours=CACHE_TTL_HOURS)
                and _cache['posts']):
            return _cache['posts']

    # Fetch outside lock to avoid blocking
    try:
        resp = requests.get(FEED_URL, timeout=10, headers={
            'User-Agent': 'ParraMacro/1.0'
        })
        resp.raise_for_status()
        posts = _parse_rss(resp.text)
    except Exception as e:
        logger.error(f"Failed to fetch Substack feed: {e}")
        with _lock:
            return _cache['posts']  # Return stale cache on error

    with _lock:
        _cache['posts'] = posts
        _cache['fetched_at'] = datetime.utcnow()

    logger.info(f"Fetched {len(posts)} posts from Substack RSS")
    return posts
