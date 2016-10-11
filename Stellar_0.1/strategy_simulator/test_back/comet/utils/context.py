# -*- coding: utf-8 -*-
__author__ = 'MoroJoJo'


from functools import wraps
from contextlib import contextmanager


'''
模拟执行的上下文
'''


class ContextStack(object):
    def __init__(self):
        self.stack = []

    def push(self, obj):
        self.stack.append(obj)

    def pop(self):
        try:
            return self.stack.pop()
        except IndexError:
            raise RuntimeError("stack is empty")

    @contextmanager
    def pushed(self, obj):
        self.push(obj)
        try:
            yield self
        finally:
            self.pop()

    @property
    def top(self):
        try:
            return self.stack[-1]
        except IndexError:
            raise RuntimeError("stack is empty")


class ExecutionContext(object):
    stack = ContextStack()

    def __init__(self, simulation_executor, phase, bar_dict=None):
        """init

        :param TradeSimulationExecutor simulation_executor:
        :param EXECUTION_PHASE phase: current execution phase
        :param BarMap bar_dict: current bar dict
        """
        self.simulation_executor = simulation_executor
        self.phase = phase
        self.bar_dict = bar_dict

    def _push(self):
        self.stack.push(self)

    def _pop(self):
        popped = self.stack.pop()
        if popped is not self:
            raise RuntimeError("Popped wrong context")
        return self

    def __enter__(self):
        self._push()
        return self

    def __exit__(self, _type, _value, _tb):
        """
        Restore the algo instance stored in __enter__.
        """
        self._pop()

    @classmethod
    def get_active(cls):
        return cls.stack.top

    @classmethod
    def get_simulation_context(cls):
        ctx = cls.get_active()
        return ctx.simulation_executor.strategy_context

    @classmethod
    def get_simulation_executor(cls):
        ctx = cls.get_active()
        return ctx.simulation_executor

    @classmethod
    def get_trade_simulation(cls):
        return cls.get_simulation_executor().trade_simulation

    @classmethod
    def get_current_dt(cls):
        return cls.get_simulation_executor().current_dt

    @classmethod
    def get_current_bar_dict(cls):
        ctx = cls.get_active()
        return ctx.bar_dict

    @classmethod
    def get_trading_params(cls):
        return cls.get_trade_simulation().trading_params

    @classmethod
    def enforce_phase(cls, *phases):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                if cls.get_active().phase not in phases:
                    raise RuntimeError(
                        "You can only call %s when executing %s" % (
                            func.__name__,
                            ", ".join(map(lambda x: x.name.lower(), phases))
                        )
                    )
                return func(*args, **kwargs)
            return wrapper
        return decorator
