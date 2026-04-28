"""
/api/v1/* — programmatic JSON surface for algotrader bots and integrations.

Authentication: Bearer-token only (`Authorization: Bearer pk_live_...`).
Cookie sessions are NOT honored here — keeps an attacker who steals a
browser cookie out of the read-only data surface, and keeps a bot that
loses its key out of the dashboard. Returns JSON 401 on auth failures
(no redirect to /auth/login).

Mount point: app.py registers this blueprint at /api/v1.
"""

from backend.api_v1.routes import api_v1_bp

__all__ = ['api_v1_bp']
