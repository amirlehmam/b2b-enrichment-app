"""
Utilitaire de rate limiting pour les APIs
"""
import time
from functools import wraps
from collections import deque


class RateLimiter:
    """Simple rate limiter basé sur une fenêtre glissante"""

    def __init__(self, max_calls: int, period: float):
        """
        Args:
            max_calls: Nombre max d'appels autorisés
            period: Période en secondes
        """
        self.max_calls = max_calls
        self.period = period
        self.calls = deque()

    def wait_if_needed(self):
        """Attend si nécessaire pour respecter la limite"""
        now = time.time()

        # Nettoyer les appels anciens
        while self.calls and self.calls[0] < now - self.period:
            self.calls.popleft()

        if len(self.calls) >= self.max_calls:
            # Attendre que le plus ancien appel expire
            sleep_time = self.calls[0] - (now - self.period)
            if sleep_time > 0:
                print(f"Rate limit atteint, attente de {sleep_time:.1f}s...")
                time.sleep(sleep_time)

        self.calls.append(time.time())


def rate_limited(max_calls: int, period: float):
    """
    Décorateur pour limiter le taux d'appels d'une fonction.

    Args:
        max_calls: Nombre max d'appels
        period: Période en secondes

    Usage:
        @rate_limited(max_calls=10, period=60)  # 10 appels par minute
        def my_api_call():
            ...
    """
    limiter = RateLimiter(max_calls, period)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            limiter.wait_if_needed()
            return func(*args, **kwargs)
        return wrapper

    return decorator


# Limiteurs pré-configurés pour chaque API
pappers_limiter = RateLimiter(max_calls=100, period=60)  # 100/min
enrich_crm_limiter = RateLimiter(max_calls=30, period=60)  # 30/min
phantombuster_limiter = RateLimiter(max_calls=10, period=60)  # 10/min
captely_limiter = RateLimiter(max_calls=50, period=60)  # 50/min
