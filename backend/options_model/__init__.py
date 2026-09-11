"""Equity volatility / options model.

A random-walk engine for a single high-volatility name (default TSEM, Tower
Semiconductor): estimate the volatility process, simulate the price forward,
turn the simulated terminal distribution into direction and strike
probabilities, and score the option structures those probabilities imply.
"""
