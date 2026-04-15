"""Pytest config for AetherGrid."""
import pytest
import asyncio

pytest_plugins = ("pytest_asyncio",)

@pytest.fixture
def state_machine():
    from aethergrid.core import ProcessTableState
    return ProcessTableState(shard_id=0)
