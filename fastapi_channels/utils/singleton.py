from typing import Type, TypeVar


T = TypeVar('T')

def singleton(cls: Type[T]) -> Type[T]:
    """Decorator that makes a class a singleton.
    
    The first time the class is instantiated, it creates an instance.
    Subsequent instantiations return the same instance.
    """
    instances = {}
    original_new = cls.__new__
    _init_called = set()
    
    def __new__(cls, *args, **kwargs):
        if cls not in instances:
            if original_new is object.__new__:
                instances[cls] = object.__new__(cls)
            else:
                instances[cls] = original_new(cls, *args, **kwargs)
        return instances[cls]
    
    original_init = cls.__init__
    
    def __init__(self, *args, **kwargs):
        if self.__class__ not in _init_called:
            _init_called.add(self.__class__)
            original_init(self, *args, **kwargs)
    
    cls.__new__ = staticmethod(__new__)
    cls.__init__ = __init__
    return cls